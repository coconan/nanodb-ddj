nanodb-ddj
==========

Our implementation of NanoDB for CS122 (Caltech's database systems implementation class).

Team Members:

- Daniel Kong
- Daniel Wang
- Jerry Zhang

**Setting up Git**

Since we are using multiple remote repositories, I'm using this setup (output from `git remote -v`):

```
cms	dkong@login.cms.caltech.edu:/cs/courses/cs122/wi1415/nanodb-ddj (fetch)
cms	dkong@login.cms.caltech.edu:/cs/courses/cs122/wi1415/nanodb-ddj (push)
nanodb	dkong@login.cms.caltech.edu:/cs/courses/cs122/wi1415/nanodb (fetch)
nanodb	dkong@login.cms.caltech.edu:/cs/courses/cs122/wi1415/nanodb (push)
origin	https://github.com/dkong1796/nanodb-ddj.git (fetch)
origin	https://github.com/dkong1796/nanodb-ddj.git (push)
```

This way, running the usual commands (like `git push origin master`) updates the Github repository, and when we want to submit, one of us should run `git push cms master`.

**Submitting an Assignment**

1. Merge the branch into master and update your local branch.
2. Tag the merge commit with `hwX`.
3. Update the designdoc with the commit hash and tag name.
4. `git push cms master`
5. `git push cms --tags`
6. `git push origin --tags`
7. Upload the designdoc to Moodle.
