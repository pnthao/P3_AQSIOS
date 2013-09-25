#ifndef _PARTN_WIN_
#define _PARTN_WIN_

/**
 * @file           partn_win.h
 * @date           May 30, 2004
 * @brief          Partition window operator
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _PARTN_WIN_SYN_
#include "execution/synopses/partn_win_syn.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _REL_SYN_
#include "execution/synopses/rel_syn.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {
	class PartnWindow : public Operator {
	private:
		
		/// System-wide unique identifier
		unsigned int id;
		
		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;
				
		/// Window size
		unsigned int numRows;
		
		/// Synopsis storing the current window of tuples
		PartnWindowSynopsis *partnWindow;
		
		/// Storeage allocator for output tuples
		StorageAlloc *outStore;

		/// Storage allocator that produces the inputs
		StorageAlloc *inStore;
		
		/// Evaluation context
		EvalContext *evalContext;
		
		/// Evaluator to make a copy of the input tuple
		AEval *copyEval;
				
		/// Timestamp of last input input element
		Timestamp lastInputTs;
		
		/// Timestamp of last output element
		Timestamp lastOutputTs;
		
		/// Are we stalled?
		bool bStalled;
		
		/// We stalled when trying to enqueue stalledElement
		Element stalledElement;
		
		static const unsigned int INPUT_ROLE = 2;
		static const unsigned int COPY_ROLE = 3;
		
	public:
		PartnWindow (unsigned int id, std::ostream &LOG);
		virtual ~PartnWindow ();
		
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setWindowSize (unsigned int numRows);
		int setEvalContext (EvalContext *evalContext);
		int setWindowSynopsis (PartnWindowSynopsis *partnWinSynopsis);
		int setOutStore (StorageAlloc *store);
		int setInStore (StorageAlloc *store);
		int setCopyEval (AEval *copyEval);
		
		int run(TimeSlice timeSlice);
		//load manager, by Thao Pham
		int run_with_shedder (TimeSlice timeSlice);
		//end of load manager, by Thao Pham

		//HR implementation by Lory Al Moakar
		/**
		 * This method is used in order to calculate the local 
		 * selectivity and the local cost per tuple of this operator
		 * @return   bool true or false
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

		
	private:
		int expireOldestTuple (Tuple partn);
	};
}

		
#endif
