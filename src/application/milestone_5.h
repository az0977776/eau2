#include <unistd.h>

#include "application.h"
#include "../util/string.h"
#include "../util/config.h"

/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set : public Object {
    public:  
        bool* vals_;  // owned; data
        size_t size_; // number of elements
        size_t num_elements_; // num true

        /** Creates a set of the same size as the dataframe. */ 
        Set(DataFrame* df) : Set(df->nrows()) { }

        /** Creates a set of the given size. */
        Set(size_t sz) :  vals_(new bool[sz]), size_(sz) {
            num_elements_ = 0;
            for(size_t i = 0; i < size_; i++) {
                vals_[i] = false; 
            }
        }

        ~Set() { delete[] vals_; }

        /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
         *  values can occur if there are references to pids or uids in commits
         *  that did not appear in projects or users.
         */
        void set(size_t idx) {
            if (idx >= size_ || vals_[idx]) {
                return; // ignoring out of bound writes
            }
            vals_[idx] = true;  
            num_elements_++;
        }

        /** Is idx in the set?  See comment for set(). */
        bool test(size_t idx) {
            if (idx >= size_) {
                return true;
            } // ignoring out of bound reads
            return vals_[idx];
        }

        size_t size() { return size_; }

        size_t num_true() { return num_elements_; }

        /** Performs set union in place. */
        void union_(Set& from) {
            abort_if_not(from.size_ == size_, "Union has two differently sized sets");
            for (size_t i = 0; i < from.size_; i++) {
                if (from.test(i)) {
                    set(i);
                }
            }
        }
};


/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Reader {
    public:
        Set& set_; // set to update
        
        SetUpdater(Set& set): set_(set) {}

        /** Assume a row with at least one column of type I. Assumes that there
         * are no missing. Reads the value and sets the corresponding position.
         * The return value is irrelevant here. */
        bool visit(Row & row) { 
            set_.set(row.get_int(0));  
            return false; 
        }

};

/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * dataframe. The data contains all the values in the set. The dataframe has
 * at least one integer column.
 ****************************************************************************/
class SetWriter: public Writer {
    public:
        Set& set_; // set to read from
        int i_ = 0;  // position in set

        SetWriter(Set& set): set_(set) { }

        /** Skip over false values and stop when the entire set has been seen */
        bool done() {
            while (i_ < set_.size_ && set_.test(i_) == false) {
                ++i_;
            }
            return i_ == set_.size_;
        }

        void visit(Row & row) { row.set(0, i_++); }
};

/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    project_id X written_user_id x commited_user_id
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Reader {
    public:
    Set& uSet; // set of collaborator 
    Set& pSet; // set of projects of collaborators
    Set newProjects;  // newly tagged collaborator projects

    ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
        uSet(uSet), pSet(pSet), newProjects(proj) {}

    /** The data frame must have at least two integer columns. The newProject
     * set keeps track of projects that were newly tagged (they will have to
     * be communicated to other nodes). */
    bool visit(Row & row) override {
        int pid = row.get_int(0);
        int uid = row.get_int(1);
        if (uSet.test(uid)) {
            if (!pSet.test(pid)) {
                pSet.set(pid);
                newProjects.set(pid);
            }
        }
        return false;
    }
};

/***************************************************************************
 * The UserTagger is a reader that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer. 
 *************************************************************************/
class UsersTagger : public Reader {
    public:
        Set& pSet;
        Set& uSet;
        Set newUsers;

        UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
            pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }

        bool visit(Row & row) override {
            int pid = row.get_int(0);
            int uid = row.get_int(1);
            if (pSet.test(pid)) {
                if(!uSet.test(uid)) {
                    uSet.set(uid);
                    newUsers.set(uid);
                }
            }
            return false;
        }
};

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
    public:
        int DEGREES = 4;  // How many degrees of separation form linus?
        int LINUS = 4967;   // The uid of Linus (offset in the user df)
        const char* PROJ = "data/projects.ltgt";
        const char* USER = "data/users.ltgt";
        const char* COMM = "data/commits.ltgt";
        DataFrame* projects; //  pid x project name  -- 'IS'
        DataFrame* users;  // uid x user name        -- 'IS'
        DataFrame* commits;  // pid x uid x uid      -- 'III'  -- project x author x commiter
        Set* uSet; // Linus' collaborators
        Set* pSet; // projects of collaborators

        Linus(KVStore& kvs) : Application(kvs) {}

        /** Compute DEGREES of Linus.  */
        void run_() override {
            readInput();
            for (size_t i = 0; i < DEGREES; i++) step(i);
            print("Milestone5: DONE\n");
        }

        /** Node 0 reads three files, cointainng projects, users and commits, and
         *  creates thre dataframes. All other nodes wait and load the three
         *  dataframes. Once we know the size of users and projects, we create
         *  sets of each (uSet and pSet). We also output a data frame with a the
         *  'tagged' users. At this point the dataframe consists of only
         *  Linus. **/
        void readInput() {
            Key pK("projs");
            Key uK("usrs");
            Key cK("comts");
            if (this_node() == 0) {
                pln("Reading...");
                projects = fromFile(PROJ, pK.clone(), &kv);
                print("    %zu projects\n", projects->nrows());

                users = fromFile(USER, uK.clone(), &kv);
                print("    %zu users\n", users->nrows());

                commits = fromFile(COMM, cK.clone(), &kv);
                print("    %zu commits\n", commits->nrows());
                
                // This dataframe contains the id of Linus.
                delete DataFrame::fromScalar(new Key("users-0-0"), &kv, LINUS);
            } else {
                projects = getAndWait(pK);
                users = getAndWait(uK);
                commits = getAndWait(cK);
            }
            uSet = new Set(users);
            pSet = new Set(projects);
        }

        /** Performs a step of the linus calculation. It operates over the three
         *  datafrrames (projects, users, commits), the sets of tagged users and
         *  projects, and the users added in the previous round. */
        void step(int stage) {
            print("Stage %d\n", stage);
            // Key of the shape: users-stage-0
            String* s0 = StrBuff().c("users-").c(stage).c("-0").get();
            Key uK(s0->c_str());
            delete s0;
            // A df with all the users added on the previous round
            DataFrame* newUsers = getAndWait(uK);    

            Set delta(users); // create a set of size users
            SetUpdater updater(delta);  
            newUsers->map(updater); // all of the new users are copied to delta.
            delete newUsers;

            ProjectsTagger project_tagger(delta, *pSet, projects);
            commits->local_map(project_tagger); // marking all projects touched by delta
            print("    About to merge new Projects\n");
            merge(project_tagger.newProjects, "projects-", stage); // node-0 waits for all other nodes to finish their stage, then merges
            // move the updated newProjects into this nodes project set
            pSet->union_(project_tagger.newProjects); 

            UsersTagger user_tagger(project_tagger.newProjects, *uSet, users);
            commits->local_map(user_tagger);
            print("    About to merge new Users\n");
            merge(user_tagger.newUsers, "users-", stage + 1);
            uSet->union_(user_tagger.newUsers);
            
            print("    after stage %d:\n", stage);
            print("        tagged projects: %zu / %zu\n", pSet->num_true(), pSet->size());
            print("        tagged users: %zu / %zu\n", uSet->num_true(), uSet->size());
        }

        /** Gather updates to the given set from all the nodes in the systems.
         * The union of those updates is then published as dataframe.  The key
         * used for the output is of the form "name-stage-0" where name is either
         * 'users' or 'projects', stage is the degree of separation being
         * computed.
         */ 
        void merge(Set& set, char const* name, int stage) {
            if (this_node() == 0) {
                for (size_t i = 1; i < config_.CLIENT_NUM; ++i) {
                    String *s1 = StrBuff().c(name).c(stage).c("-").c(i).get();
                    Key nK(s1->c_str());
                    delete s1;

                    DataFrame* delta = getAndWait(nK);
                    print("    received delta of %zu elements from node %zu\n", delta->nrows(), i);

                    SetUpdater upd(set);
                    delta->map(upd);
                    delete delta;
                }
                // once all other nodes have put in their delta's create the master DataFrame
                print("    storing %zu / %zu merged elements\n", set.num_true(), set.size());
                SetWriter writer(set);
                String* k_str = StrBuff().c(name).c(stage).c("-0").get();
                Key k(k_str->c_str());
                delete k_str;
                DataFrame* df_1 = DataFrame::fromVisitor(&k, &kv, "I", writer);
                delete df_1;
            } else {
                print("    sending %zu / %zu elements to master node\n", set.num_true(), set.size());
                SetWriter writer(set);
                String* s2 = StrBuff().c(name).c(stage).c("-").c(this_node()).get();
                Key k(s2->c_str());
                delete s2;
                delete DataFrame::fromVisitor(&k, &kv, "I", writer);

                // wait for the full master list from node 0
                String* s3 = StrBuff().c(name).c(stage).c("-0").get();
                Key mK(s3->c_str());
                delete s3;
                DataFrame* merged = getAndWait(mK);
                print("    receiving %zu merged elements\n", merged->nrows());
                SetUpdater upd(set);
                merged->map(upd);
                delete merged;
            }
        }
}; // Linus

