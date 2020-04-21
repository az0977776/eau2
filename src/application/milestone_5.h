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
                return false; // out of bounds means it is not in the set
            } // ignoring out of bound reads
            return vals_[idx];
        }

        size_t size() { return size_; }

        size_t num_true() { return num_elements_; }

        /** Performs set union in place. */
        void union_(Set& from) {
            abort_if_not(from.size_ <= size_, "Union has two differently sized sets");
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
        size_t i_ = 0;  // position in set

        SetWriter(Set& set): set_(set) { }

        /** Skip over false values and stop when the entire set has been seen */
        bool done() {
            while (i_ < set_.size_ && set_.test(i_) == false) {
                ++i_;
            }
            return i_ == set_.size_;
        }

        void visit(Row & row) { row.set(0, (int) i_++); }
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
        uSet(uSet), pSet(pSet), newProjects(proj) { }

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
        size_t DEGREES = 7;  // How many degrees of separation form linus?
        // int LINUS = 4967;   // The uid of Linus (offset in the user df)
        // const char* PROJ = "data/projects.ltgt";
        // const char* USER = "data/users.ltgt";
        // const char* COMM = "data/commits.ltgt";      

        int LINUS = 0;
        const char* PROJ = "data/generated_projects2.txt";
        const char* USER = "data/generated_users2.txt";
        const char* COMM = "data/generated_commits2.txt";

        // int LINUS = 0;
        // const char* PROJ = "data/7stage_3x3_generated_projects.ltgt";
        // const char* USER = "data/7stage_3x3_generated_users.ltgt";
        // const char* COMM = "data/7stage_3x3_generated_commits.ltgt";

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

            // delta should only have 1 person in it

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




/*
LINUS on 4 stages whole files:
-------------------------------------------
NODE 0:     125486231 projects
NODE 0:     32411734 users
NODE 0:     117936711 commits
NODE 0: Stage 0
NODE 2: Stage 0
NODE 1: Stage 0
NODE 3: Stage 0
NODE 0:     About to merge new Projects
NODE 1:     About to merge new Projects
NODE 1:     sending 1643 / 125486231 elements to master node
NODE 2:     About to merge new Projects
NODE 2:     sending 1438 / 125486231 elements to master node
NODE 3:     About to merge new Projects
NODE 3:     sending 1495 / 125486231 elements to master node
NODE 0:     received delta of 1643 elements from node 1
NODE 0:     received delta of 1438 elements from node 2
NODE 0:     received delta of 1495 elements from node 3
NODE 0:     storing 6164 / 125486231 merged elements
NODE 1:     receiving 6164 merged elements
NODE 2:     receiving 6164 merged elements
NODE 3:     receiving 6164 merged elements
NODE 0:     About to merge new Users
NODE 1:     About to merge new Users
NODE 1:     sending 21241 / 32411734 elements to master node
NODE 2:     About to merge new Users
NODE 2:     sending 19633 / 32411734 elements to master node
NODE 3:     About to merge new Users
NODE 3:     sending 20926 / 32411734 elements to master node
NODE 0:     received delta of 21241 elements from node 1
NODE 0:     received delta of 19633 elements from node 2
NODE 0:     received delta of 20926 elements from node 3
NODE 0:     storing 36627 / 32411734 merged elements
NODE 0:     after stage 0:
NODE 0:         tagged projects: 6164 / 125486231
NODE 0:         tagged users: 36627 / 32411734
NODE 0: Stage 1
NODE 1:     receiving 36627 merged elements
NODE 2:     receiving 36627 merged elements
NODE 1:     after stage 0:
NODE 1:         tagged projects: 6164 / 125486231
NODE 1:         tagged users: 36627 / 32411734
NODE 1: Stage 1
NODE 2:     after stage 0:
NODE 2:         tagged projects: 6164 / 125486231
NODE 2:         tagged users: 36627 / 32411734
NODE 2: Stage 1
NODE 3:     receiving 36627 merged elements
NODE 3:     after stage 0:
NODE 3:         tagged projects: 6164 / 125486231
NODE 3:         tagged users: 36627 / 32411734
NODE 3: Stage 1
NODE 1:     About to merge new Projects
NODE 1:     sending 348783 / 125486231 elements to master node
NODE 2:     About to merge new Projects
NODE 2:     sending 338350 / 125486231 elements to master node
NODE 3:     About to merge new Projects
NODE 3:     sending 340244 / 125486231 elements to master node
NODE 0:     About to merge new Projects
NODE 0:     received delta of 348783 elements from node 1
NODE 0:     received delta of 338350 elements from node 2
NODE 0:     received delta of 340244 elements from node 3
NODE 0:     storing 1212262 / 125486231 merged elements
NODE 1:     receiving 1212262 merged elements
NODE 2:     receiving 1212262 merged elements
NODE 3:     receiving 1212262 merged elements
NODE 0:     About to merge new Users
NODE 2:     About to merge new Users
NODE 2:     sending 522681 / 32411734 elements to master node
NODE 1:     About to merge new Users
NODE 1:     sending 523301 / 32411734 elements to master node
NODE 3:     About to merge new Users
NODE 3:     sending 507727 / 32411734 elements to master node
NODE 0:     received delta of 523301 elements from node 1
NODE 0:     received delta of 522681 elements from node 2
NODE 0:     received delta of 507727 elements from node 3
NODE 0:     storing 1450973 / 32411734 merged elements
NODE 0:     after stage 1:
NODE 0:         tagged projects: 1218426 / 125486231
NODE 0:         tagged users: 1487600 / 32411734
NODE 0: Stage 2
NODE 1:     receiving 1450973 merged elements
NODE 2:     receiving 1450973 merged elements
NODE 3:     receiving 1450973 merged elements
NODE 1:     after stage 1:
NODE 1:         tagged projects: 1218426 / 125486231
NODE 1:         tagged users: 1487600 / 32411734
NODE 2:     after stage 1:
NODE 2:         tagged projects: 1218426 / 125486231
NODE 2:         tagged users: 1487600 / 32411734
NODE 1: Stage 2
NODE 2: Stage 2
NODE 3:     after stage 1:
NODE 3:         tagged projects: 1218426 / 125486231
NODE 3:         tagged users: 1487600 / 32411734
NODE 3: Stage 2
NODE 0:     About to merge new Projects
NODE 2:     About to merge new Projects
NODE 2:     sending 4951899 / 125486231 elements to master node
NODE 1:     About to merge new Projects
NODE 1:     sending 5021627 / 125486231 elements to master node
NODE 3:     About to merge new Projects
NODE 3:     sending 4949844 / 125486231 elements to master node
NODE 0:     received delta of 5021627 elements from node 1
NODE 0:     received delta of 4951899 elements from node 2
NODE 0:     received delta of 4949844 elements from node 3
NODE 0:     storing 18012852 / 125486231 merged elements
NODE 2:     receiving 18012852 merged elements
NODE 3:     receiving 18012852 merged elements
NODE 1:     receiving 18012852 merged elements
NODE 0:     About to merge new Users
NODE 2:     About to merge new Users
NODE 2:     sending 1192238 / 32411734 elements to master node
NODE 1:     About to merge new Users
NODE 1:     sending 1208246 / 32411734 elements to master node
NODE 3:     About to merge new Users
NODE 3:     sending 1159763 / 32411734 elements to master node
NODE 0:     received delta of 1208246 elements from node 1
NODE 0:     received delta of 1192238 elements from node 2
NODE 0:     received delta of 1159763 elements from node 3
NODE 0:     storing 3488007 / 32411734 merged elements
NODE 0:     after stage 2:
NODE 0:         tagged projects: 19231278 / 125486231
NODE 0:         tagged users: 4975607 / 32411734
NODE 0: Stage 3
NODE 1:     receiving 3488007 merged elements
NODE 2:     receiving 3488007 merged elements
NODE 3:     receiving 3488007 merged elements
NODE 1:     after stage 2:
NODE 1:         tagged projects: 19231278 / 125486231
NODE 1:         tagged users: 4975607 / 32411734
NODE 1: Stage 3
NODE 2:     after stage 2:
NODE 2:         tagged projects: 19231278 / 125486231
NODE 2:         tagged users: 4975607 / 32411734
NODE 2: Stage 3
NODE 3:     after stage 2:
NODE 3:         tagged projects: 19231278 / 125486231
NODE 3:         tagged users: 4975607 / 32411734
NODE 3: Stage 3
NODE 0:     About to merge new Projects
NODE 2:     About to merge new Projects
NODE 2:     sending 4122277 / 125486231 elements to master node
NODE 1:     About to merge new Projects
NODE 1:     sending 4116960 / 125486231 elements to master node
NODE 3:     About to merge new Projects
NODE 3:     sending 4149273 / 125486231 elements to master node
NODE 0:     received delta of 4116960 elements from node 1
NODE 0:     received delta of 4122277 elements from node 2
NODE 0:     received delta of 4149273 elements from node 3
NODE 0:     storing 15615372 / 125486231 merged elements
NODE 2:     receiving 15615372 merged elements
NODE 1:     receiving 15615372 merged elements
NODE 3:     receiving 15615372 merged elements
NODE 0:     About to merge new Users
NODE 2:     About to merge new Users
NODE 2:     sending 722301 / 32411734 elements to master node
NODE 1:     About to merge new Users
NODE 1:     sending 732535 / 32411734 elements to master node
NODE 3:     About to merge new Users
NODE 3:     sending 731608 / 32411734 elements to master node
NODE 0:     received delta of 732535 elements from node 1
NODE 0:     received delta of 722301 elements from node 2
NODE 0:     received delta of 731608 elements from node 3
NODE 0:     storing 2313602 / 32411734 merged elements
NODE 0:     after stage 3:
NODE 0:         tagged projects: 34846650 / 125486231
NODE 0:         tagged users: 7289209 / 32411734
NODE 0: Milestone5: DONE
NODE 2:     receiving 2313602 merged elements
NODE 1:     receiving 2313602 merged elements
NODE 3:     receiving 2313602 merged elements
NODE 2:     after stage 3:
NODE 2:         tagged projects: 34846650 / 125486231
NODE 2:         tagged users: 7289209 / 32411734
NODE 2: Milestone5: DONE
NODE 1:     after stage 3:
NODE 1:         tagged projects: 34846650 / 125486231
NODE 1:         tagged users: 7289209 / 32411734
NODE 1: Milestone5: DONE
NODE 3:     after stage 3:
NODE 3:         tagged projects: 34846650 / 125486231
NODE 3:         tagged users: 7289209 / 32411734
NODE 3: Milestone5: DONE
*/