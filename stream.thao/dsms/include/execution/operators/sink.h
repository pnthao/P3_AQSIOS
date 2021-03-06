#ifndef _SINK_
#define _SINK_

/**
 * @file     sink.h
 * @date     Dec. 6, 2004
 * @brief    The sink operator - an operator that consumes its input
 *           but produces no output
 */

#include <ostream>
using std::ostream;

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

namespace Execution {
	class Sink : public Operator {
	private:
		/// System-wide unique id
		unsigned int   id;

		/// System log
		ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Storage allocator who allocated tuples in the input
		StorageAlloc *inStore;
		
	public:
		Sink (unsigned int id, ostream &LOG);
		virtual ~Sink ();

		//------------------------------------------------------------
		// Initialization
		//------------------------------------------------------------
		
		int setInputQueue (Queue *inputQueue);
		int setInStore (StorageAlloc *store);
		int run (TimeSlice timeSlice);
		//load manager, by Thao Pham
		int run_with_shedder (TimeSlice timeSlice);
		//end of load manager, by Thao Pham
		//HR implementation by Lory Al Moakar
		/**
		 * This method is used in order to calculate the local 
		 * selectivity and the local cost per tuple of this operator
		 * @return bool true or false
		 * true when the statistics have changed and false otherwise 
		 */ 
		bool calculateLocalStats();
		
		/** this method is used in order to calculate the priority
		 * of the operator. This is used in order to assign the priorities
		 * used by the scheduler to schedule operators.
		 */
		
		void refreshPriority();
		// end of part 1 of HR implementation by LAM
		//HR with ready by Lory Al Moakar
		int readyToExecute();
		//end of part 1 of HR with ready by LAM

		//ArMaDILoS, by Thao Pham
		void deactivate();
		//end of ArmaDILoS
	};
}

#endif
