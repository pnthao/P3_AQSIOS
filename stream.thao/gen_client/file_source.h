#ifndef _FILE_SOURCE_
#define _FILE_SOURCE_

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _CPP_QUEUE_
#include <queue>
#endif
#include <fstream>
#include <iostream>

using Interface::TableSource;
using namespace std;

namespace Client {
	
	class FileSource : public TableSource {
	private:
		
		/// Source file input
		std::fstream input;
		
		/// Maximum size of tuples we support
		static const unsigned int MAX_TUPLE_SIZE = 256;
		
		/// Buffer for tuples
		char tupleBuf [MAX_TUPLE_SIZE];
		
		enum Type {
			INT, FLOAT, CHAR, BYTE
		};
		
		/// Types of attributes
		Type attrTypes [MAX_ATTRS];
		
		/// Attr lengthts
		int attrLen [MAX_ATTRS];
		
		/// Offsets of attributes in the tupleBuf
		int offsets [MAX_ATTRS];
		
		/// Number of attributes
		int numAttrs;		

		/// Length of tuples
		int tupleLen;

		//added by Thao Pham to load all source data to memory
		std::queue<char*> rawDataBuf;
		//end of part 1 added by Thao Pham to load all source data to memory
	public:
		FileSource (const char *fileName);
		~FileSource ();
		
		int start ();
		int getNext (char *&tuple, unsigned int &len, bool &isHeartbeta);
		// Response Time Calculation By Lory Al Moakar
		int getNext (char *&tuple, unsigned int &len, bool& isHeartbeat, 
			     unsigned long long int &cursysTime);
		//added by Thao Pham to load all source data to memory
		int getNext (char *&tuple, unsigned int &len, bool& isHeartbeat,
					     unsigned long long int &cursysTime, bool dataInBuffer);

		int loadSourceData();
		//ArmaDiLos
		virtual streampos getCurPos();
		virtual Timestamp startDataReading(std::streampos curPos, bool noseek = false);

		//end of part 2 added by Thao Pham to load all source data to memory

		//end of part 1 of response time calculation by LAM
		//load management, by Thao Pham
		void skip(int num_of_tuples); 
		
		int end ();
		
	private:
		int parseTuple (char *lineBuffer);
		int parseSchema (char *lineBuffer);
		int computeOffsets ();
	};
}

#endif
