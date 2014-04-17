#ifndef _ROW_WIN_
#define _ROW_WIN_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _WIN_SYN_
#include "execution/synopses/win_syn.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {
	
	class RowWindow : public Operator {
	private:
		
		/// System-wide unique id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;

		/// Storage alloc which allocates input tupes
		StorageAlloc *inStore;
		
		/// Size of the window
		unsigned int windowSize;
		
		/// Window synopsis
		WindowSynopsis *winSynopsis;

		/// Timestamp of the last input tuple
		Timestamp lastInputTs;

		/// Timestamp of the last output tuple
		Timestamp lastOutputTs;

		/// MIN (Number of tuples processed so far, windowSize)
		unsigned int numProcessed;
		
		//----------------------------------------------------------------------
		// Stall logic
		//----------------------------------------------------------------------
		
		/// Are we stalled?
		bool bStalled;

		/// We stalled trying to enqueue this element
		Element stalledElement;

	public:

		RowWindow (unsigned int id, std::ostream &LOG);
		virtual ~RowWindow ();

		//----------------------------------------------------------------------
		// Initialization routines
		//----------------------------------------------------------------------		
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setWindowSize (unsigned int numRows);
		int setWindowSynopsis (WindowSynopsis *winSynopsis);
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

		//ArmaDILoS, by Thao Pham
		int run_in_stop_preparing(TimeSlice timeSlice);
		void deactivate();

		//end of ArmaDILoS
		
	private:
		bool clearStall();
	};
}

#endif
