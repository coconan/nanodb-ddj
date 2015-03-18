package edu.caltech.nanodb.indexes;


import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.EventDispatchException;
import edu.caltech.nanodb.server.RowEventListener;
import edu.caltech.nanodb.storage.HashedTupleFile;
import edu.caltech.nanodb.storage.SequentialTupleFile;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class implements the {@link RowEventListener} interface to make sure
 * that all indexes on an updated table are kept up-to-date.  This handler is
 * installed by the {@link StorageManager#initialize} setup method.
 */
public class IndexUpdater implements RowEventListener {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexUpdater.class);


    /**
     * A cached reference to the index manager since we use it a lot in this
     * class.
     */
    private IndexManager indexManager;


    public IndexUpdater(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.indexManager = storageManager.getIndexManager();
    }


    @Override
    public void beforeRowInserted(TableInfo tblFileInfo, Tuple newValues) {
        // Ignore.
    }


    @Override
    public void afterRowInserted(TableInfo tblFileInfo, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowUpdated(TableInfo tblFileInfo, Tuple oldTuple,
                                 Tuple newValues) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowUpdated(TableInfo tblFileInfo, Tuple oldValues,
                                Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowDeleted(TableInfo tblFileInfo, Tuple oldTuple) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowDeleted(TableInfo tblFileInfo, Tuple oldValues) {
        // Ignore.
    }


    /**
     * This helper method handles the case when a tuple is being added to the
     * table, after the row has already been added to the table.  All indexes
     * on the table are updated to include the new row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the new tuple that was inserted into the table
     */
    private void addRowToIndexes(TableInfo tblFileInfo, PageTuple ptup) {
        logger.debug("Adding tuple " + ptup + " to indexes for table " +
            tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        Set<Integer> notNull = schema.getNotNull();

        for (int i = 0; i < schema.numColumns(); i++) {
            // Check NOT_NULL constraints and raise exception if they are violated
            if (notNull.contains(i)) {
                if (ptup.getColumnValue(i) == null) {
                    try {
                        tblFileInfo.getTupleFile().deleteTuple(ptup);
                    } catch (IOException e) {
                        throw new IllegalStateException
                                ("Tried to delete a tuple that violated a constraint but failed: "
                                + schema.getColumnInfo(i).getName());
                    }
                    throw new IllegalArgumentException
                            ("Cannot insert NULL values into columns with NOT_NULL constraint: "
                            + schema.getColumnInfo(i).getName());
                }
            }
        }
        try {

            for (ColumnRefs indexDef : schema.getIndexes().values()) {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                        indexDef.getIndexName());
                // Check if there are unique constraints on the columns of the index
                if (schema.hasKeyOnColumns(indexDef)) {
                    Tuple tofind = IndexUtils.makeSearchKeyValue(indexDef, ptup, false);
                    // If there is already a tuple with these values in the index
                    if (IndexUtils.findTupleInIndex(tofind, indexInfo.getTupleFile()) != null) {

                        // Since the tuple was already inserted, remove it and throw an exception
                        try {
                            tblFileInfo.getTupleFile().deleteTuple(ptup);
                        } catch (IOException e) {
                            throw new IllegalStateException
                                    ("Tried to delete a tuple that violated a constraint but failed: "
                                            + schema.getColumnInfo(indexDef.getCol(0)).getName());
                        }
                        throw new IllegalArgumentException
                                ("Cannot insert duplicate values into columns with PRIMARY or UNIQUE constraints: "
                                        + schema.getColumnInfo(indexDef.getCol(0)).getName());
                    }
                }
            }

            for (ColumnRefs indexDef : schema.getIndexes().values()) {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                        indexDef.getIndexName());

                // Insert the new tuple into the index
                Tuple toInsert = IndexUtils.makeSearchKeyValue(indexDef, ptup, true);
                indexInfo.getTupleFile().addTuple(toInsert);
            }

        }
        catch (IOException e) {
            throw new EventDispatchException("Couldn't update indexes table " +
                    tblFileInfo.getTableName(), e);
        }
}


    /**
     * This helper method handles the case when a tuple is being removed from
     * the table, before the row has actually been removed from the table.
     * All indexes on the table are updated to remove the row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the tuple about to be removed from the table
     */
    private void removeRowFromIndexes(TableInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Removing tuple " + ptup + " from indexes for table " +
            tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnRefs indexDef : schema.getIndexes().values()) {
            try {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                // Create a copy of the tuple to be deleted
                Tuple toDelete = IndexUtils.makeSearchKeyValue(indexDef, ptup, true);

                if (IndexUtils.findTupleInIndex(toDelete, indexInfo.getTupleFile()) == null) {
                    throw new IllegalStateException("Tried to delete tuple from index but it does not exist?");
                }

                indexInfo.getTupleFile().deleteTuple(toDelete);
            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName());
            }
        }
    }
}
