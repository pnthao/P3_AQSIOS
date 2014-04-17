#ifndef _SELECT_
#define _SELECT_

/**
 * @file         select.h
 * @date         May 30, 2004
 * @brief        Selection operator
 */

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _BEVAL_
#include "execution/internals/beval.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _CPP_OSTREAM
#include <ostream>
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

namespace Execution {
	class Select : public Operator {
	private:
		/// System-wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Input queue
		Queue *inputQueue;
		
		/// Output queue
		Queue *outputQueue;

		/// "Context" in which the selection predicate is evaluated. [[
		/// point to some general description of evaluation contexts ]]
		EvalContext *evalContext;
		
		/// The selection predicate
		BEval *predicate;

		/// Storeage alloc who allocs the input tuples
		StorageAlloc *inStore;
		
		/// Timestamp of the last element dequeued from input queue.  0 if
		/// no tuple has been dequeued.  Note: an element might correspond
		/// to a data tuple or a heartbeat.
		Timestamp lastInputTs;
		
		/// Timestamp of the last element enqueued in the output queue. 0
		/// if no such element has been enqueued.
		Timestamp lastOutputTs;
		
		/// [[ Explanation ]] Note: same as INPUT_CONTEXT in inst_select.cc
		static const unsigned int INPUT_CONTEXT = 2;
		
	public:
		Select(unsigned int id, std::ostream &LOG);
		virtual ~Select();
		
		//----------------------------------------------------------------------
		// Functions for initializing state
		//----------------------------------------------------------------------
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setInStore (StorageAlloc *inStore);		
		int setPredicate (BEval *predicate);
		int setEvalContext (EvalContext *evalContext);
		
		int run (TimeSlice timeSlice); 
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
		
		//by Thao Pham, to simulate the cost variation
		int simulated_cost;
		unsigned long long int cost_effective_time;
		unsigned long long int ctrl_last_ts;
		FILE *f_cost_simulation;
		int getDelayTime();
		//end of cost simulation by Thao Pham

		//ArmaDILoS, by Thao Pham
		void deactivate();
		//end of ArmaDILoS by Thao Pham
	};
}

#endif
