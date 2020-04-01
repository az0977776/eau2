#include <unistd.h>
#include <stdio.h>

#include "../src/application.h"

#include "../src/dataframe/row.h"
#include "../src/dataframe/reader_writer.h"

class Num : public Object {
    public:
        int val_;
    
        Num() : Num(0) { }
        Num(int val) { val_ = val; }

        void inc() { val_++; }
        int get() { return val_; }

        bool equal(Object* o) {
            Num* n = dynamic_cast<Num*>(o);
            if (n == nullptr) {
                return false;
            }
            return n->val_ == val_;
        }
};

class FileReader : public Writer {
    public:
        static const size_t BUFSIZE = 1024;

        char * buf_;
        size_t end_ = 0;
        size_t i_ = 0;
        FILE * file_;

        /** Creates the reader and opens the file for reading.  */
        FileReader(const char* filename) : Writer() {
            file_ = fopen(filename, "r");
            abort_if_not(file_ != nullptr, "Cannot open file %s", filename);
            buf_ = new char[BUFSIZE + 1]; //  null terminator
            fillBuffer_();
            skipWhitespace_();
        }

        /** Reads next word and stores it in the row. Actually read the word.
         While reading the word, we may have to re-fill the buffer  */
        void visit(Row & r) override {
            assert(i_ < end_);
            assert(! isspace(buf_[i_]));
            size_t wStart = i_;
            while (true) {
                if (i_ == end_) {
                    if (feof(file_)) { ++i_;  break; }
                    i_ = wStart;
                    wStart = 0;
                    fillBuffer_();
                }
                if (isspace(buf_[i_]))  break;
                ++i_;
            }
            buf_[i_] = 0;
            String word(buf_ + wStart, i_ - wStart);
            r.set(0, &word);
            ++i_;
            skipWhitespace_();
        }
    
        /** Returns true when there are no more words to read.  There is nothing
         more to read if we are at the end of the buffer and the file has
        all been read.     */
        bool done() override { return (i_ >= end_) && feof(file_);  }
    
        /** Reads more data from the file. */
        void fillBuffer_() {
            size_t start = 0;
            // compact unprocessed stream
            if (i_ != end_) {
                start = end_ - i_;
                memcpy(buf_, buf_ + i_, start);
            }
            // read more contents
            end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
            i_ = start;
        }
    
        /** Skips spaces.  Note that this may need to fill the buffer if the
            last character of the buffer is space itself.  */
        void skipWhitespace_() {
            while (true) {
                if (i_ == end_) {
                    if (feof(file_)) return;
                    fillBuffer_();
                }
                // if the current character is not whitespace, we are done
                if (!isspace(buf_[i_]))
                    return;
                // otherwise skip it
                ++i_;
            }
        }
};
 
 
/****************************************************************************/
// convert a dataframe with schema('S') to a HashMap of String -> count
class Adder : public Reader {
    public:
        Map<String,Num>& map_;  // String to Num map;  Num holds an int
        
        Adder(Map<String, Num>& map) : Reader(), map_(map) {}
        
        bool visit(Row& r) override {
            // get the string from the dataframe
            String* word = r.get_string(0);  // Who owns this Adder or dataframe? TODO
            abort_if_not(word != nullptr, "Adder got a string that was nullptr");

            // increment the ocunt of the word in the map
            Num* count = map_.get(word);
            if (count == nullptr) {
                count = new Num(); // if it does not exist, then the count of that word is 0
                map_.add(word->clone(), count);
            }
            count->inc(); // increment
            return false;
        }
};
 
/***************************************************************************/
// convert a HashMap of String -> count to a Dataframe with schema('SI')
class Summer : public Writer {
    public:
        Map<String,Num>& map_;
        String** keys_;
        size_t index_ = 0;

        Summer(Map<String,Num>& map) : Writer(), map_(map) {
            keys_ = map_.keys();
        }

        ~Summer() {
            delete[] keys_;
        }

        void visit(Row& r) override {
            abort_if_not(!done(), "Summer tried to visit a row after it was done");
            String* key = keys_[index_];
            Num* value = map_.get(key);

            r.set(0, key);
            r.set(1, *((int *) value));
            index_++;
        }

        bool done() override { return index_ == map_.size(); }
};
 
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount: public Application {
    public:
        static const size_t BUFSIZE = 1024;
        Key in;
        KeyBuff kbuf;
        // Map<String,size_t> all;
        const char* filename_;

        WordCount(const char* filename) : Application(), in("data"), kbuf(new Key(0, "wc-map-")) { 
            filename_ = filename;
        }

        /** The master nodes reads the input, then all of the nodes count. */
        void run_() override {
            printf("starting to run, node: %zu\n", this_node());
            if (this_node() == 0) {
                FileReader fr(filename_);
                DataFrame* df = DataFrame::fromVisitor(&in, &kv, "S", fr);
                df->print();
                delete df;
            }
            local_count();
            reduce();
        }

        /** Returns a key for given node.  These keys are homed on master node
         *  which then joins them one by one. */
        Key* mk_key(size_t idx) {
            Key * k = kbuf.get(idx);
            printf("Created key: ");
            k->print();
            return k;
        }

        /** Compute word counts on the local node and build a data frame. */
        void local_count() {
            printf("waiting for the filereader on node 0 to finish\n");
            DataFrame* words = getAndWait(in); // Dataframe of schema "S" with every word
            p("Node ").p(this_node()).pln(": starting local count...");

            // count up all words that are on this node (store in map)
            Map<String,Num> map;
            Adder add(map);  // Adder is a Reader
            words->local_map(add);  // local map takes a Rower
            // created hashmap of S->I
            delete words;

            // convert map of S-> I to dataframe with schema "SI"
            Summer cnt(map);  // a writer
            delete DataFrame::fromVisitor(mk_key(this_node()), &kv, "SI", cnt);

            map.delete_and_clear_items();
        }

        /** Merge the data frames of all nodes */
        void reduce() {
            if (this_node() != 0) 
                return;
            pln("Node 0: reducing counts...");
            Map<String, Num> map;
            Key* own = mk_key(0);
            merge(get(*own), map);
            for (size_t i = 1; i < CLIENT_NUM; ++i) { // merge other nodes
                Key* ok = mk_key(i);
                merge(getAndWait(*ok), map);
                delete ok;
            }
            p("Different words: ").pln(map.size());
            delete own;

            map.delete_and_clear_items();
        }

        void merge(DataFrame* df, Map<String,Num>& m) {
            Adder add(m);
            df->map(add);
            delete df;
        }
}; // WordcountDemo


int main() {
    WordCount wc("data/word_count.txt");

    wc.run_();

    // this is to keep the program running
    sleep(20);

    return 0;
}