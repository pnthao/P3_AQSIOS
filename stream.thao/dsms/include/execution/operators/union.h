#ifndef _UNION_
#define _UNION_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _CPP_OSTREAM
#include <ostream>
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _LIN_SYN_
#include "execution/synopses/lin_syn.h"
#endif

#ifndef _EVAL_CONTEXT_
#include "execution/internals/eval_context.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

namespace Execution {
	class Union : public Operator {
	private:
		/// System-wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		/// Left input queue
		Queue *leftInQueue;
		
		/// Right input queue
		Queue *rightInQueue;		
		
		/// Output queue
		Queue *outputQueue;
		
		/// Storage allocator for the output
		StorageAlloc *outStore;
		
		/// Synopsis for storing the output
		LineageSynopsis *outSyn;
		
		/// Storage allocator who allocs left input tuples
		StorageAlloc *leftInStore;
		
		/// Storage allocator who allocs right input tuples
		StorageAlloc *rightInStore;

		/// Evaluation context
		EvalContext *evalContext;

		/// Evaluator to construct output tuples from left
		AEval *leftOutEval;
		
		/// Evaluator to construct output tuples from right
		AEval *rightOutEval;
		
		/// Timestamp of last left element
		Timestamp lastLeftTs;
		
		/// Timestamp of the last right element
		Timestamp lastRightTs;
		
		/// Timestamp of last output element
		Timestamp lastOutputTs;

		static const unsigned int LEFT_ROLE = 2;
		static const unsigned int RIGHT_ROLE = 3;
		static const unsigned int OUTPUT_ROLE = 4;
		
	public:
		Union (unsigned int id, std::ostream &LOG);
		virtual ~Union ();
		
		//----------------------------------------------------------------------
		// Functions for initializing state
		//----------------------------------------------------------------------
		int setLeftInputQueue (Queue *inputQueue);
		int setRightInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setOutStore (StorageAlloc *store);
		int setOutSyn (LineageSynopsis *outSyn);
		int setLeftInputStore (StorageAlloc *store);
		int setRightInputStore (StorageAlloc *store);
		int setEvalContext (EvalContext *evalContext);
		int setLeftOutEval (AEval *eval);
		int setRightOutEval (AEval *eval);
		
		int run(TimeSlice timeSlice);
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

		//TODO: add ArmaDILoS implementation here


	private:
		int handleLeftPlus (Element element);
		int handleRightPlus (Element element);		
		int handleLeftMinus (Element element);
		int handleRightMinus (Element element);
	};
}

#endif
