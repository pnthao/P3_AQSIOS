/**
 * @file         heavyweight_drop.h
 * @date         Feb 21, 2009
 * @brief        a "heavyweight" drop operator that allocates new space for output tuples
 * @author		 Thao Pham
 */ 

#ifndef _HEAVYWEIGHT_DROP_
#define _HEAVYWEIGHT_DROP_

#ifndef _DROP_
#include "execution/operators/drop.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution{
class HeavyWeightDrop : public Drop
{
private:
	private:
/// System log
	std::ostream &LOG;
	//Storage alloc which allocates space for output tuples
	StorageAlloc *outStore;
	
	/// Evaluation context for various computations
	EvalContext *evalContext;
		
	/// Evaluator that copies a tuple in the input store to the
	/// output
	AEval *copyEval;
	
	static const unsigned int INPUT_ROLE = 2;
	static const unsigned int OUTPUT_ROLE = 3;
	
public:
	HeavyWeightDrop(unsigned int id, std::ostream &LOG);
	virtual ~HeavyWeightDrop();
	
	int setOutStore (StorageAlloc *outStore);
	int setEvalContext (EvalContext *evalContext);
	int setCopyEval (AEval *eval);
	
	//run for timeSlice (maximum number of tuples processed in a scheduling cycle)
	int run (TimeSlice timeSlice); 
	//load manager, by Thao Pham
	int run_with_shedder (TimeSlice timeSlice);
	//end of load manager, by Thao Pham
};
}
#endif /*_HEAVYWEIGHT_DROP_*/
