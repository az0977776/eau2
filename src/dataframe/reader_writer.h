#include "object.h"
#include "row.h"


class Writer : public Object {
    public:
        /** Reads next word and stores it in the row. Actually read the word.
         While reading the word, we may have to re-fill the buffer  */
        virtual void visit(Row & r) = 0;

        /** Returns true when there are no more words to read.  There is nothing
         more to read if we are at the end of the buffer and the file has
        all been read.     */
        virtual bool done() = 0;
};

// just to satisify the milestone 4 code naming
class Reader : public Rower {
    public:
        bool accept(Row& r) { return visit(r); }
        virtual bool visit(Row& r) = 0;
};