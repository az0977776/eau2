#include <gtest/gtest.h>

#include "test_macros.h"
#include "../../src/application/milestone_5.h"
#include "../../src/dataframe/dataframe.h"

// *************************** Set Tests ***********************************

void test_set() {
    Set s(15);

    EXPECT_EQ(s.size(), 15);
    EXPECT_EQ(s.num_true(), 0);
    
    for (size_t i = 0; i < s.size(); i++) {
        EXPECT_FALSE(s.test(i));
    }

    Set s2(10);

    EXPECT_EQ(s2.size(), 10);
    EXPECT_EQ(s2.num_true(), 0);

    for (size_t i = 2; i < 7; i++) {
        s2.set(i);
    }
    
    EXPECT_EQ(s2.size(), 10);
    EXPECT_EQ(s2.num_true(), 5);    

    s.union_(s2);
    EXPECT_EQ(s.size(), 15);
    EXPECT_EQ(s.num_true(), 5);  
}

TEST(testLinus, testSet) {
    test_set();
}


// *************************** SetUpdater Tests ***********************************

void test_set_updater() {
    size_t num = 500;
    size_t multiple = 3;
    int* arr = new int[num];
    for (size_t i = 0; i < num; i++) {
        arr[i] = (int) (i * multiple);
    }
    
    Key key(0, "some key");
    KVStore kvs(false);
    DataFrame* df = DataFrame::fromArray(&key, &kvs, num, arr);
    Set set(num * multiple);
    SetUpdater set_updater(set);

    df->local_map(set_updater);

    ASSERT_EQ(num, set.num_true());

    for (size_t i = 0; i < set.size(); i++) {
        if (i % multiple == 0) {
            EXPECT_TRUE(set.test(i));
        } else {
            EXPECT_FALSE(set.test(i));
        }
    }
    delete df;
}

TEST(testLinus, testSetUpdater) {
    test_set_updater();
}


// *************************** SetWriter Tests ***********************************

void test_set_writer() {
    size_t num = 500;
    size_t multiple = 3;

    Key key(0, "some key");
    KVStore kvs(false);

    Set set(num * multiple);
    for (size_t i = 0; i < num; i++) {
        set.set(i * multiple);
    }
    SetWriter writer(set);
    
    DataFrame* df = DataFrame::fromVisitor(&key, &kvs, "I", writer);

    ASSERT_EQ(df->nrows(), num);

    for (size_t i = 0; i < num; i++) {
        EXPECT_EQ(i * multiple, df->get_int(0, i));
    }
    delete df;
}

TEST(testLinus, testSetWriter) {
    test_set_writer();
}

// *************************** ProjectsTagger Tests ***********************************

DataFrame* build_commit_df(Key* k, KVStore* kvs, size_t size, Schema &s, int* project_ids, int* author_ids, int* commit_ids) {
    DataFrame* df = new DataFrame(s, *k, kvs, false);
    Row r(s);

    for (size_t i = 0; i < size; i++) {
        r.set(0, project_ids[i]);
        r.set(1, author_ids[i]);
        r.set(2, commit_ids[i]);
        df->add_row(r, false, false); // try not to send anything to kv store, operate on cached chunk
    }
    
    df->commit(); // commits the latest chunks and adds the dataframe to the kv store
    
    return df;
}

void test_projects_tagger() {
    size_t init_users = 100;
    size_t init_projects = 50;
    size_t num = 500;
    int* projects = new int[num];
    int* authors = new int[num];
    int* commiters = new int[num];

    for (size_t i = 0; i < num; i++) {
        projects[i] = (int) (i);
        authors[i] = (int) (i);
        commiters[i] = (int) (i);
    }
    
    Key key(0, "some key");
    KVStore kvs(false);
    Schema schema("III");
    DataFrame* df = build_commit_df(&key, &kvs, num, schema, projects, authors, commiters);

    Set uSet(df);
    for (size_t i = 0; i < init_users; i++) {
        uSet.set(i);
    }

    Set pSet(df);
    for (size_t i = 0; i < init_projects; i++) {
        pSet.set(i);
    }
    
    // newProjects should be the other 50
    ProjectsTagger project_tagger(uSet, pSet, df);
    df->local_map(project_tagger);

    ASSERT_EQ(init_users - init_projects, project_tagger.newProjects.num_true());
    for (size_t i = 0; i < num; i++) {
        if (i >= init_projects && i < init_users) {
            EXPECT_TRUE(project_tagger.newProjects.test(i));
        } else {
            EXPECT_FALSE(project_tagger.newProjects.test(i));
        }   
    }
    delete df;
}

TEST(testLinus, testProjectsTagger) {
    test_projects_tagger();
}

// *************************** UsersTagger Tests ***********************************

void test_users_tagger() {
    size_t init_users = 50;
    size_t init_projects = 100;
    size_t num = 500;
    int* projects = new int[num];
    int* authors = new int[num];
    int* commiters = new int[num];

    for (size_t i = 0; i < num; i++) {
        projects[i] = (int) (i);
        authors[i] = (int) (i);
        commiters[i] = (int) (i);
    }
    
    Key key(0, "some key");
    KVStore kvs(false);
    Schema schema("III");
    DataFrame* df = build_commit_df(&key, &kvs, num, schema, projects, authors, commiters);

    Set uSet(df);
    for (size_t i = 0; i < init_users; i++) {
        uSet.set(i);
    }

    Set pSet(df);
    for (size_t i = 0; i < init_projects; i++) {
        pSet.set(i);
    }
    
    // newProjects should be the other 50
    UsersTagger user_tagger(pSet, uSet, df);
    df->local_map(user_tagger);

    ASSERT_EQ(init_projects - init_users, user_tagger.newUsers.num_true());
    for (size_t i = 0; i < num; i++) {
        if (i >= init_users && i < init_projects) {
            EXPECT_TRUE(user_tagger.newUsers.test(i));
        } else {
            EXPECT_FALSE(user_tagger.newUsers.test(i));
        }   
    }
    delete df;
}

TEST(testLinus, testUsersTagger) {
    test_users_tagger();
}

