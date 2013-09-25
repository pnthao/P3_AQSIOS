
/**
 * @file         drop.h
 * @date         Feb 21, 2009
 * @brief        Drop operator
 * @author		 Thao Pham
 */ 

#ifndef _DROP_
#define _DROP_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
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

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

#ifndef _STREAM_SOURCE_
#include "execution/operators/stream_source.h"
#endif

#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif

#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif


namespace Execution{
class Drop : public Operator
{
protected:
	/// System-wide unique id
	unsigned int id;
	
	/// Input queue
	Queue *inputQueue;
	
	/// Output queue
	Queue *outputQueue;

	/// Storage alloc which allocates input tupes 
	StorageAlloc *inStore;
	
	/// Timestamp of the last element dequeued from input queue.  0 if
	/// no tuple has been dequeued.
	Timestamp lastInputTs;
		
	/// Timestamp of the last element enqueued in the output queue. 0
	// if no such element has been enqueued.
	Timestamp lastOutputTs;
	
	/*this drop is active or not? drop percent = 0 means not active 
	 *this is not used in this version 
	 */
	//bool active;
	
	
	/*these two variables are used to keep track of number of tuples randomly dropped so far
	 *to make sure that exactly "dropPercent" percent of tuples are dropped.
	 *This might not be necessary though
	 *The variables is reset whenever tuplecount = 100 
	 */
	unsigned int dropCount;
	unsigned int tupleCount;
	
	/*array of related inputs (inputs that flow into this drop)*/
	Physical::Operator* relatedInputs[MAX_OPS_PER_TYPE];
	unsigned int numRelatedInputs;
	
	 /* array of partial selectivities, for each related input, this is the product 
	  * of selectivities of all the operators along the path 
	  * from that input to the drop. These are used to compute the input rate
	  * flowing into the drop and then the saving achieved by this drop.
	  * This array is coupled with the relatedInputs array (orders must match) 
	  * This is not used in this version.
	  */
	 double partSelectivity [MAX_OPS_PER_TYPE];
	
	/* Array of related outputs (outputs that are affected by this drop) 
	 * This is not used in this version*/
	
	Physical::Operator* relatedOutputs[MAX_OPS_PER_TYPE];
	unsigned int numRelatedOutputs;
	
	
	/*load coefficient of the flow falling into the subgraph after the drop
	 * (in current model, a drop always has 1 output)
	 * This is not used in this version.
	 */
	double loadCoefficient;
	 
	

public:
	virtual ~Drop();
	
	/// drop probability (percent)
	unsigned int dropPercent;
	
	int setInputQueue (Queue *inputQueue);
	int setOutputQueue (Queue *outputQueue);
	int setInStore (StorageAlloc *inStore);	
	int setDropPercent(unsigned int _dropPercent);	
	int addRelatedInput(Physical::Operator*& op);
	int addRelatedOutput(Physical::Operator*& op);
	Physical::Operator** getRelatedOutputs();
	int getNumOfRelatedOutputs();
	Physical::Operator** getRelatedInputs();
	int getNumOfRelatedInputs();
	
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
	
	//int activate(bool _active);
	
	//run for timeSlice (maximum number of tuples processed in a scheduling cycle)
	//int run (TimeSlice timeSlice); 
	

};

}

#endif /*_DROP_*/
