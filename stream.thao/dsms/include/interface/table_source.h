#ifndef _TABLE_SOURCE_
#define _TABLE_SOURCE_
#include<iostream>
using namespace std;
namespace Interface {

/**
 * The interface that the STREAM server uses to get input stream /
 * relation tuples.  Various kinds of inputs can be obtained by extending
 * this class (e.g, read from a file, read from a network etc)
 *
 * The main method is getNext(), which teh server uses to pull the next
 * tuple whenever it desires.  
 *
 * Before  consuming any  input  tuples, the  server  invokes the  start()
 * method,   which   can   be   used   to   perform   various   kinds   of
 * initializations. Similarly, the end() method is called ,when the server
 * is not going to invoke any more getNext()s.
 */
	
	class TableSource {
	public:	
		virtual ~TableSource() {}	
		
		/**
		 * Signal that getNext will be invoked next.
		 *
		 * @return            0 (success), !0 (failure)
		 */
		virtual int start() = 0;
		
		/**
		 * Get the next tuple from the table (stream/relation).		 
		 *
		 * @param   tuple     0 (if the next tuple has not yet available)
		 *                    pointer to the location of the next tuple.
		 *                    A tuple is just a pointer to a memory location
		 *                    containing the encoding of the tuple
		 *                    attribute.   Memory is owned by TableSource
		 *                    is guaranteed to be valid only till the
		 *                    next getNext call.
		 *
		 *                    Modified (Dec 6): you can pass heartbeats
		 *                    by setting isHeartbeat to 'true'. Then
		 *                    the first 4 bytes of the tuple should contains
		 *                    the timestamp as usual.
		 *
		 * @param   len       Length of the returned tuple
		 * @return            0 (success), !0 (failure)
		 */
		
		virtual int getNext(char *& tuple, unsigned int& len,
							bool& isHeartbeat) = 0;
		
		// Response Time Calculation By Lory Al Moakar
		/**
		 * Get the next tuple from the table (stream/relation).		 
		 *
		 * @param   tuple     0 (if the next tuple has not yet available)
		 *                    pointer to the location of the next tuple.
		 *                    A tuple is just a pointer to a memory location
		 *                    containing the encoding of the tuple
		 *                    attribute.   Memory is owned by TableSource
		 *                    is guaranteed to be valid only till the
		 *                    next getNext call.
		 *
		 *                    Modified (Dec 6): you can pass heartbeats
		 *                    by setting isHeartbeat to 'true'. Then
		 *                    the first 4 bytes of the tuple should contains
		 *                    the timestamp as usual.
		 *
		 * @param   len       Length of the returned tuple
		 * @param cursysTime  the current system time so that no tuples 
		 *                    enter the system with timestamps in the future
		 * @return            0 (success), !0 (failure)
		 */

		virtual int getNext (char *&tuple, unsigned int &len, bool& isHeartbeat, 
			     unsigned long long int &cursysTime) = 0;
		//end of part 1 of response time calculation by LAM
		
		//added by Thao Pham, to simulate the case the input tuples is read from memory buffer
		virtual int getNext (char *&tuple, unsigned int &len, bool& isHeartbeat,
						 unsigned long long int &cursysTime, bool dataInBuffer) = 0;
		virtual int loadSourceData() =0;

		//for ArmaDilos
		virtual streampos getCurPos() = 0;

		//end of part 1 added by Thao Pham
		/**
		 * Signal that the server needs no more tuples.
		 * 
		 * @return            0 (success), !0 (failure)
		 */
		 
		//load management, by Thao Pham
		virtual void skip(int num_of_tuples)=0; 
		// end of load management by Thao Pham
		virtual int end() = 0;
	};
}

#endif
