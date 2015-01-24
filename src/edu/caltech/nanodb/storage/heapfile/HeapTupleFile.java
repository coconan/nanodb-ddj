package edu.caltech.nanodb.storage.heapfile;


import java.io.EOFException;
import java.io.IOException;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class implements the TupleFile interface for heap files.
 *
 * We keep track of pages that are partially filled (have space for additional
 * tuples) by maintaining a singly linked list of such data pages. Each page
 * has a pointer (the page number) to the next page in the list. The end of
 * the list is indicated by having a pointer to 0 (the header page).
 */
public class HeapTupleFile implements TupleFile {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeapTupleFile.class);


    /**
     * The storage manager to use for reading and writing file pages, pinning
     * and unpinning pages, write-ahead logging, and so forth.
     */
    private StorageManager storageManager;


    /** The schema of tuples in this tuple file. */
    private TableSchema schema;


    /** Statistics for this tuple file. */
    private TableStats stats;


    /** Page number of head of non-full page list. */
    private int firstNonFullPage;


    /** The file that stores the tuples. */
    private DBFile dbFile;


    public HeapTupleFile(StorageManager storageManager, DBFile dbFile,
                         TableSchema schema, TableStats stats,
                         int firstNonFullPage) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        this.storageManager = storageManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
        this.firstNonFullPage = firstNonFullPage;
    }


    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public TableStats getStats() {
        return stats;
    }


    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if
     * there are no tuples in the file.
     */
    @Override
    public Tuple getFirstTuple() throws IOException {
        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.

            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page...

                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;

                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    return new HeapFilePageTuple(schema, dbPage, iSlot, offset);
                }

                // If we got here, the page has no tuples.  Unpin the page.
                dbPage.unpin();
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                         ".  Returning null.");
        }

        return null;
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by many other operations in the database, such as
     * indexes.
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     */
    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBPage dbPage;
        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                fptr.getPageNo() + " doesn't exist in file " +
                dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = DataPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = DataPage.getSlotValue(dbPage, slot);
        if (offset == DataPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot +
                " on page " + fptr.getPageNo() + " is empty.");
        }

        return new HeapFilePageTuple(schema, dbPage, slot, offset);
    }


    /**
     * Returns the tuple that follows the specified tuple,
     * or <tt>null</tt> if there are no more tuples in the file.
     **/
    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {

        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();

        if (dbPage == null) {
            throw new IllegalArgumentException("NULL DBPAGE, TUPLE INVALID?");
        }

        DBFile dbFile = dbPage.getDBFile();

        int nextSlot = ptup.getSlot() + 1;
        while (true) {
            int numSlots = DataPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != DataPage.EMPTY_SLOT) {
                    return new HeapFilePageTuple(schema, dbPage, nextSlot,
                                                 nextOffset);
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            try {
                DBPage nextDBPage =
                    storageManager.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                dbPage.unpin();
                dbPage = nextDBPage;

                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                // Unpin the final dbPage
                dbPage.unpin();
                return null;
            }
        }

        // It's pretty gross to have no return statement here, but there's
        // no way to reach this point.
    }


    /**
     * Adds the specified tuple into the table file.  A new
     * <tt>HeapFilePageTuple</tt> object corresponding to the tuple is returned.
     *
     * @review (donnie) This could be made a little more space-efficient.
     *         Right now when computing the required space, we assume that we
     *         will <em>always</em> need a new slot entry, whereas the page may
     *         contain empty slots.  (Note that we don't always create a new
     *         slot when adding a tuple; we will reuse an empty slot.  This
     *         inefficiency is simply in estimating the size required for the
     *         new tuple.)
     */
    @Override
    public Tuple addTuple(Tuple tup) throws IOException {

        /*
         * Check to see whether any constraints are violated by
         * adding this tuple
         *
         * Find out how large the new tuple will be, so we can find a page to
         * store it.
         *
         * Find a page with space for the new tuple.
         *
         * Generate the data necessary for storing the tuple into the file.
         */

        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize +
                " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // Search for a page to put the tuple in. If we hit the end of the
        // list of partially filled pages (pageNo will be 0), then create a
        // new page.
        int pageNo = firstNonFullPage;
        DBPage dbPage = null;
        // Keep track of the previous page so we can maintain the list.
        DBPage prevPage = null;
        while (pageNo > 0) {
            dbPage = storageManager.loadDBPage(dbFile, pageNo);

            int freeSpace = DataPage.getFreeSpaceInPage(dbPage);

            logger.trace(String.format("Page %d has %d bytes of free space.",
                         pageNo, freeSpace));

            // If this page has enough free space to add a new tuple, break
            // out of the loop.  (The "+ 2" is for the new slot entry we will
            // also need.)
            if (freeSpace >= tupSize + 2) {
                logger.debug("Found space for new tuple in page " + pageNo + ".");
                break;
            }

            // If we reached this point then the page doesn't have enough
            // space, so go on to the next data page.
            if (prevPage != null) {
                prevPage.unpin();
            }
            prevPage = dbPage;
            pageNo = DataPage.getNextNonFullPage(dbPage);
            dbPage = null;
        }

        if (dbPage == null) {
            // Get the page number of the next allocated page.
            pageNo = dbFile.getNumPages();
            logger.debug("Creating new page " + pageNo + " to store new tuple.");
            dbPage = storageManager.loadDBPage(dbFile, pageNo, true);
            DataPage.initNewPage(dbPage);

            // Add new page to the list of non-full pages.
            if (prevPage == null) {
                firstNonFullPage = pageNo;
            } else {
                DataPage.setNextNonFullPage(prevPage, pageNo);
            }
        }

        int slot = DataPage.allocNewTuple(dbPage, tupSize);
        int tupOffset = DataPage.getSlotValue(dbPage, slot);

        logger.debug(String.format(
            "New tuple will reside on page %d, slot %d with offset %d.",
                pageNo, slot, tupOffset));

        HeapFilePageTuple pageTup =
            HeapFilePageTuple.storeNewTuple(schema, dbPage, slot, tupOffset, tup);

        // If there is no more space for a new tuple, then remove this page
        // from the list of non-full pages. For tuples with variable lengths,
        // it may be possible for a page to fit some tuples but not others.
        // However, instead of trying to compute an estimated size, we will
        // simply use this current tuple as a rough estimate of the size
        // needed. That is, will this tuple fit in the page again or not?
        if (DataPage.getFreeSpaceInPage(dbPage) < tupSize + 2) {
            // Set this page to "full".
            DataPage.setPageIsFull(dbPage, true);

            // Remove this from the list of non-full pages.
            int nextNonFullPage = DataPage.getNextNonFullPage(dbPage);
            if (prevPage == null) {
                firstNonFullPage = nextNonFullPage;
            } else {
                DataPage.setNextNonFullPage(prevPage, nextNonFullPage);
            }
        }

        if (prevPage != null) {
            prevPage.unpin();
        }

        // Update values that may have changed.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        HeaderPage.setFirstNonFullPage(headerPage, firstNonFullPage);
        headerPage.unpin();

        DataPage.sanityCheck(dbPage);

        // Done with the page, so we can unpin it.
        dbPage.unpin();

        return pageTup;
    }


    // Inherit interface-method documentation.
    /**
     * @review (donnie) This method will fail if a tuple is modified in a way
     *         that requires more space than is currently available in the data
     *         page.  One solution would be to move the tuple to a different
     *         page and then perform the update, but that would cause all kinds
     *         of additional issues.  So, if the page runs out of data, oh well.
     */
    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();

            int colIndex = schema.getColumnIndex(colName);
            ptup.setColumnValue(colIndex, value);
        }

        DBPage dbPage = ptup.getDBPage();
        DataPage.sanityCheck(dbPage);
    }


    // Inherit interface-method documentation.
    @Override
    public void deleteTuple(Tuple tup) throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        DataPage.deleteTuple(dbPage, ptup.getSlot());

        // Add this to the list of non-full pages if it was not already in the
        // list.
        if (DataPage.getPageIsFull(dbPage)) {
            DataPage.setPageIsFull(dbPage, false);

            // Insert the page at the front of the list.
            DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
            int nextNonFullPage = HeaderPage.getFirstNonFullPage(headerPage);
            HeaderPage.setFirstNonFullPage(headerPage, dbPage.getPageNo());
            DataPage.setNextNonFullPage(dbPage, nextNonFullPage);
            headerPage.unpin();
        }

        DataPage.sanityCheck(dbPage);
    }

    @Override
    public void analyze() throws IOException {
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public List<String> verify() throws IOException {
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public void optimize() throws IOException {
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
