#include <unistd.h>
#include <stdio.h>

#include "../src/application.h"
#include "../src/dataframe/row.h"

class Writer : public Rower {
    
};

class Reader : public Rower {
    
};


class FileReader : public Writer {
    public:
        static const size_t BUFSIZE = 1024;

        char * buf_;
        size_t end_ = 0;
        size_t i_ = 0;
        FILE * file_;

        /** Creates the reader and opens the file for reading.  */
        FileReader(const char* filename) {
            file_ = fopen(filename, "r");
            if (file_ == nullptr) fail("Cannot open file %s", filename);
            buf_ = new char[BUFSIZE + 1]; //  null terminator
            fillBuffer_();
            skipWhitespace_();
        }

        /** Reads next word and stores it in the row. Actually read the word.
         While reading the word, we may have to re-fill the buffer  */
        bool accept(Row & r) override {
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
            r.set(0, word);
            ++i_;
            skipWhitespace_();
            return true;
        }
    
        /** Returns true when there are no more words to read.  There is nothing
         more to read if we are at the end of the buffer and the file has
        all been read.     */
        bool done() { return (i_ >= end_) && feof(file_);  }
    
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
class Adder : public Reader {
    public:
        SIMap& map_;  // String to Num map;  Num holds an int
        
        Adder(SIMap& map) : map_(map)  {}
        
        bool visit(Row& r) override {
            String* word = r.get_string(0);
            assert(word != nullptr);
            Num* num = map_.contains(*word) ? map_.get(*word) : new Num();
            assert(num != nullptr);
            num->v++;
            map_.set(*word, num);
            return false;
        }
};
 
/***************************************************************************/
class Summer : public Writer {
    public:
        SIMap& map_;
        size_t i = 0;
        size_t j = 0;
        size_t seen = 0;

        Summer(SIMap& map) : map_(map) {}

        void next() {
            if (i == map_.capacity_ ) return;
            if ( j < map_.items_[i].keys_.size() ) {
                j++;
                ++seen;
            } else {
                ++i;
                j = 0;
                while( i < map_.capacity_ && map_.items_[i].keys_.size() == 0 )  i++;
                if (k()) ++seen;
            }
        }

        String* k() {
            if (i==map_.capacity_ || j == map_.items_[i].keys_.size()) return nullptr;
            return (String*) (map_.items_[i].keys_.get_(j));
        }

        size_t v() {
            if (i == map_.capacity_ || j == map_.items_[i].keys_.size()) {
                assert(false); 
                return 0;
            }
            return ((Num*)(map_.items_[i].vals_.get_(j)))->v;
        }

        void visit(Row& r) {
            if (!k()) next();
            String & key = *k();
            size_t value = v();
            r.set(0, key);
            r.set(1, (int) value);
            next();
        }

        bool done() {return seen == map_.size(); }
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
        SIMap all;

        WordCount(size_t idx, NetworkIfc & net):
        Application(idx, net), in("data"), kbuf(new Key("wc-map-",0)) { }

        /** The master nodes reads the input, then all of the nodes count. */
        void run_() override {
            if (index == 0) {
                FileReader fr;
                delete DataFrame::fromVisitor(&in, &kv, "S", fr);
            }
            local_count();
            reduce();
        }

        /** Returns a key for given node.  These keys are homed on master node
         *  which then joins them one by one. */
        Key* mk_key(size_t idx) {
            Key * k = kbuf.c(idx).get();
            LOG("Created key " << k->c_str());
            return k;
        }

        /** Compute word counts on the local node and build a data frame. */
        void local_count() {
            DataFrame* words = (kv.waitAndGet(in));
            p("Node ").p(index).pln(": starting local count...");
            SIMap map;
            Adder add(map);
            words->local_map(add);
            delete words;
            Summer cnt(map);
            delete DataFrame::fromVisitor(mk_key(index), &kv, "SI", cnt);
        }

        /** Merge the data frames of all nodes */
        void reduce() {
            if (index != 0) 
                return;
            pln("Node 0: reducing counts...");
            SIMap map;
            Key* own = mk_key(0);
            merge(kv.get(*own), map);
            for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
                Key* ok = mk_key(i);
                merge(kv.waitAndGet(*ok), map);
                delete ok;
            }
            p("Different words: ").pln(map.size());
            delete own;
        }

        void merge(DataFrame* df, SIMap& m) {
            Adder add(m);
            df->map(add);
            delete df;
        }
}; // WordcountDemo