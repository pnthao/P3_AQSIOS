#ifndef _BIN_JOIN_
#include "execution/operators/bin_join.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

//HR implementation by Lory Al Moakar
#ifndef _COMPAREOPS_
#include "execution/operators/compareOPS.h"
#endif

#include <queue>
#include <math.h>
//end of part 0 of HR implementation by LAM

#define MIN(a,b) (((a) < (b))? (a) : (b))

#define LOCK_OUT_TUPLE(t) (outStore -> addRef ((t)))
#define LOCK_INNER_TUPLE(t) (innerInputStore -> addRef ((t)))
#define LOCK_OUTER_TUPLE(t) (outerInputStore -> addRef ((t)))
#define UNLOCK_INNER_TUPLE(t) (innerInputStore -> decrRef ((t)))
#define UNLOCK_OUTER_TUPLE(t) (outerInputStore -> decrRef ((t)))

using namespace Execution;
using namespace std;

BinaryJoin::BinaryJoin (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> outerInputQueue = 0;
	this -> innerInputQueue = 0;
	this -> outputQueue = 0;
	this -> innerSynopsis = 0;
	this -> outerSynopsis = 0;
	this -> joinSynopsis = 0;
	this -> outStore = 0;
	this -> innerInputStore = 0;
	this -> outerInputStore = 0;
	this -> evalContext = 0;
	this -> outputConstructor = 0;
	this -> lastOuterTs = 0;
	this -> lastInnerTs = 0;
	this -> lastOutputTs = 0;
	this -> bStalled = false;
	this -> scanWhenStalled = 0;
	
	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = BIN_JOIN;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	this -> priority = 1;
	this -> num_tuples_processed = 0;
	this -> firstRefresh = true;
	//end of part 1 of HR implementation by LAM
	//load manager, by Thao Pham
	this->snapshot_local_cost_per_tuple = 0;
	this-> isShedder = false;
	this->input_load =0; //when isShedder is true, this operator is not a source load and therefore its input load is the input rate at the corresponding source * (product of all preceding ops)
	this->drop_percent =0;
	this->loadCoefficient =0;
    this->snapshot_loadCoefficient =0;
	this->effective_loadCoefficient =0;

#ifdef _CTRL_LOAD_MANAGE_
	ctrl_num_of_queuing_tuples = 0;
#endif //_CTRL_LOAD_MANAGE_	
	//end of load manager, by Thao Pham
}

BinaryJoin::~BinaryJoin () {
	if (evalContext)
		delete evalContext;
	if (outputConstructor)
		delete outputConstructor;
}

int BinaryJoin::setOuterInputQueue (Queue *outerQueue)
{
	ASSERT (outerQueue);

	this -> outerInputQueue = outerQueue;
	return 0;
}

int BinaryJoin::setInnerInputQueue(Queue *innerQueue)
{
	ASSERT (innerQueue);

	this -> innerInputQueue = innerQueue;
	return 0;
}

int BinaryJoin::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int BinaryJoin::setInnerSynopsis (RelationSynopsis *innerSynopsis)
{
	ASSERT (innerSynopsis);

	this -> innerSynopsis = innerSynopsis;
	return 0;
}

int BinaryJoin::setOuterSynopsis(RelationSynopsis *outerSynopsis)
{
	ASSERT (outerSynopsis);

	this -> outerSynopsis = outerSynopsis;
	return 0;
}

int BinaryJoin::setJoinSynopsis (LineageSynopsis *joinSynopsis)
{
	this -> joinSynopsis = joinSynopsis;
	return 0;
}

int BinaryJoin::setInnerScan (unsigned int innerScanId)
{
	this -> innerScanId = innerScanId;
	return 0;
}

int BinaryJoin::setOuterScan (unsigned int outerScanId)
{
	this -> outerScanId = outerScanId;
	return 0;
}

int BinaryJoin::setOutStore (StorageAlloc *outStore)
{
	ASSERT (outStore);
	
	this -> outStore = outStore;
	return 0;
}

int BinaryJoin::setOuterInputStore (StorageAlloc *store)
{
	ASSERT(store);

	this -> outerInputStore = store;
	return 0;
}

int BinaryJoin::setInnerInputStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> innerInputStore = store;
	return 0;
}

int BinaryJoin::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int BinaryJoin::setOutputConstructor (AEval *outputConstructor)
{
	ASSERT (outputConstructor);
	
	this -> outputConstructor = outputConstructor;
	return 0;
}

int BinaryJoin::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Timestamp outerMinTs, innerMinTs;
	Element outerPeekElement, innerPeekElement;
	Element outerElement, innerElement;
	
#ifdef _MONITOR_
	startTimer ();
#endif


	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM
	
	// I have a stall ... 
	if (bStalled) {
		
		if ((rc = clearStall()) != 0) 
			return rc;		
		
		// ... and can't clear it
		if (bStalled) {
			
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif
			
			return 0;
		}
	}
	
	// Minimum timestamp possible on the next outer element
	outerMinTs = lastOuterTs;
	
	// Minimum timestamp possible on the next inner element
	innerMinTs = lastInnerTs;
	
	numElements = timeSlice;
	
	unsigned int e = 0;
	
	for (  ; e < numElements && !bStalled ; e++) {
  
		// Peek to revise min timestamp estimate
		if (outerInputQueue -> peek (outerPeekElement))
			outerMinTs = outerPeekElement.timestamp;
		
		// Peek to revise min inner timestamp estimate
		if (innerInputQueue -> peek (innerPeekElement))
			innerMinTs = innerPeekElement.timestamp;
		
		// We have to process the outer if it has an element waiting in
		// the queue.  Otherwise we cannot do any processing
		if (outerMinTs < innerMinTs) {
			
		        if (!outerInputQueue -> dequeue (outerElement)) {
		            //printf("blocked join\n");
			  break;
			}
		  
		        //HR implementation by Lory Al Moakar
		        // increment the number of tuples read so far
		        num_tuples_processed += 1;
		        // end of part 6 of HR implementation by LAM
		        // update last outer ts
		        lastOuterTs = outerElement.timestamp;
		  
		        // Processing of  data tuples varies depending  on whether the
			// element is a PLUS or MINUS.  The functions processOuterPlus
			// and processOuterMinus could  lead to stalling (output queue
			// is  full while the  operator has  elements to  output).  In
			// this case, the variable bStalled is set, which terminates
			// the forloop.
			
			if (outerElement.kind == E_PLUS) {
	
#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				
				rc = processOuterPlus (outerElement);
				if (rc != 0)
					return rc;
				

			}
			
			else if (outerElement.kind == E_MINUS) {
				
#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				
				rc = processOuterMinus (outerElement);
				if (rc != 0)
					return rc;
				
				// Discard the tuple
				UNLOCK_OUTER_TUPLE(outerElement.tuple);

			}
			
			// Heartbeats require no processing
			


#ifdef _DM_
			else
			{
				ASSERT (outerElement.kind == E_HEARTBEAT);
			}
#endif			
			
		}
		
		// This is symmetric to the outer case: We have to process the
		// outer if it has an element waiting in the queue.  Otherwise we
		// cannot do any processing		
		else if (innerMinTs < outerMinTs) {

		        if (!innerInputQueue -> dequeue (innerElement)) {
			  //printf("blocked join\n");
			  
			  break;
			}
			//HR implementation by Lory Al Moakar
			// increment the number of tuples read so far
			num_tuples_processed += 1;
			// end of part 6 of HR implementation by LAM
			lastInnerTs = innerElement.timestamp;
			
			if (innerElement.kind == E_PLUS) {
	
#ifdef _CTRL_LOAD_MANAGE_
			ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				
				rc = processInnerPlus (innerElement);
				if (rc != 0)
					return rc;

				
			}
			else if (innerElement.kind == E_MINUS) {
				
#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				
				rc = processInnerMinus (innerElement);
				if (rc != 0)
					return rc;
				
				UNLOCK_INNER_TUPLE(innerElement.tuple);

			}
			
#ifdef _DM_
			else {
				ASSERT (innerElement.kind == E_HEARTBEAT);

			}
#endif

		}
		
		// innerMinTs == outerMinTs: In this case, we can pick any queue
		// that has an element.
		else if (outerInputQueue -> dequeue (outerElement)) {
		        //HR implementation by Lory Al Moakar
		        // increment the number of tuples read so far
		        num_tuples_processed += 1;
		        // end of part 6 of HR implementation by LA
			lastOuterTs = outerElement.timestamp;
			
			if (outerElement.kind == E_PLUS) {
	
#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
			
				rc = processOuterPlus (outerElement);
				if (rc != 0)
					return rc;
				

			}
			else if (outerElement.kind == E_MINUS) {
				
				#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
				#endif //_CTRL_LOAD_MANAGE	
				
				rc = processOuterMinus (outerElement);
				if (rc != 0)
					return rc;

				UNLOCK_OUTER_TUPLE(outerElement.tuple);
				
			}
			
			// Heartbeats require no processing
#ifdef _DM_
			else {
				ASSERT (outerElement.kind == E_HEARTBEAT);
				
			}
#endif
						
		}
		
		else if (innerInputQueue -> dequeue (innerElement)) {
		  
		  	//HR implementation by Lory Al Moakar
		        // increment the number of tuples read so far
		        num_tuples_processed += 1;
			// end of part 6 of HR implementation by LAM
			
			lastInnerTs = innerElement.timestamp;
			
#ifdef _CTRL_LOAD_MANAGE_
			this->ctrl_num_of_queuing_tuples -=1; //the queuing tuples for join is the sum of the 2 input queues

#endif //_CTRL_LOAD_MANAGE
			
			if (innerElement.kind == E_PLUS) {
	
#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				
				rc = processInnerPlus (innerElement);
				if (rc != 0)
					return rc;
				

			}
			else if (innerElement.kind == E_MINUS) {
				
#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				
				rc = processInnerMinus (innerElement);
				if (rc != 0)
					return rc;

				UNLOCK_INNER_TUPLE(innerElement.tuple);
				
			}
			
#ifdef _DM_
			else {
				ASSERT (innerElement.kind == E_HEARTBEAT);
			
			}
#endif
			
		}
		
		else {
		  break;
		}
	}

	
	// Heartbeat generation
	if (!outputQueue -> isFull() && (lastOutputTs < innerMinTs) &&
		(lastOutputTs < outerMinTs)) {
		ASSERT (!bStalled);
		
		lastOutputTs = MIN (outerMinTs, innerMinTs);
		
		outputQueue -> enqueue (Element::Heartbeat (lastOutputTs));
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif			

	//HR implementation by Lory Al Moakar
	//get current time
	
	unsigned long long int timeAfterLoop = mytime.getCPUTime();
	
	//calculate time elapsed during execution
	//add it to local cost
        if ( e > 0 )      
	  local_cost += timeAfterLoop - timeBeforeLoop;
	//num_tuples_processed += e;
	//end of part 4 of HR implementation by LAM

     //if all the input queues are empty and all the input operators are inactive,
      //this operator should deactivate itself.
     if(innerInputQueue->isEmpty() && outerInputQueue->isEmpty()
    		 &&inputs[0]->status==INACTIVE && inputs[1]->status==INACTIVE&&!bStalled)
    	 deactivate();
        	
	return 0;
}

/**
 * Join an outer tuple with the synopsis of the inner.  The outer tuple is
 * part of an PLUS element.  The join of the outer tuple and an inner
 * tuple if produced as a PLUS element.
 */

int BinaryJoin::processOuterPlus (Element outerElement)
{	
	int rc;

#ifdef _MONITOR_
	logInput ();
#endif
	
	// Iterator that scans the inner
	TupleIterator *innerScan;
	
	// Tuple of inner that joins with outerElement.tuple
	Tuple innerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (outerElement.tuple, OUTER_ROLE);
	
	// Insert the outer tuple into outerSynopsis
	if ((rc = outerSynopsis -> insertTuple (outerElement.tuple)) != 0)
		return rc;
	
	// Scan of inner tuples that join with outer tuple
	if ((rc = innerSynopsis -> getScan (innerScanId, innerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (innerTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processOuterMinus & 
		// processInnerMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		

#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE		

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM	
		lastOutputTs = outputElement.timestamp;
		
#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_PLUS;
		scanWhenStalled = innerScan;
		stallElement = outerElement;		
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinaryJoin::processOuterMinus (Element outerElement)
{
	int rc;

	// Tuple iterator of the inner
	// Iterator that scans the inner
	TupleIterator *innerScan;
	
	// Tuple of inner that joins with outerElement.tuple
	Tuple innerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (outerElement.tuple, OUTER_ROLE);

	// Delete the tuple from the outer synopsis
	if ((rc = outerSynopsis -> deleteTuple (outerElement.tuple)) != 0)
		return rc;
	UNLOCK_OUTER_TUPLE (outerElement.tuple);
	
	// Scan of inner tuples that join with outer tuple
	if ((rc = innerSynopsis -> getScan (innerScanId, innerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0) return rc;			
			
			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}

		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (innerTuple, INNER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE			
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 6 of HR implementation by LAM


		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_MINUS;
		scanWhenStalled = innerScan;
		stallElement = outerElement;
		LOCK_OUTER_TUPLE (stallElement.tuple);
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;
}

/**
 * Join an inner tuple with the synopsis of the outer.  The inner tuple is
 * part of an PLUS element.  The join of the inner tuple and an outer
 * tuple if produced as a PLUS element.
 */

int BinaryJoin::processInnerPlus (Element innerElement)
{	
	int rc;

#ifdef _MONITOR_
	logInput ();
#endif
	
	// Iterator that scans the outer
	TupleIterator *outerScan;
	
	// Tuple of outer that joins with innerElement.tuple
	Tuple outerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (innerElement.tuple, INNER_ROLE);

	// Insert the tuple into the inner synopsis
	if ((rc = innerSynopsis -> insertTuple (innerElement.tuple)) != 0)
		return rc;
	
	// Scan of outer tuples that join with inner tuple
	if ((rc = outerSynopsis -> getScan (outerScanId, outerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (outerTuple, OUTER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processInnerMinus & 
		// processOuterMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 7 of HR implementation by LAM		
		
		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_PLUS;
		scanWhenStalled = outerScan;
		stallElement = innerElement;
	}
	
	else {
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinaryJoin::processInnerMinus (Element innerElement)
{
	int rc;

	// Tuple iterator of the outer
	// Iterator that scans the outer
	TupleIterator *outerScan;
	
	// Tuple of outer that joins with innerElement.tuple
	Tuple outerTuple;
	
	// Joined + possibly projected output tuple
	Tuple outputTuple;
	
	// The output element
	Element outputElement;
	
	evalContext -> bind (innerElement.tuple, INNER_ROLE);
	
	// Delete tuple from the outer synopsis
	if ((rc = innerSynopsis -> deleteTuple (innerElement.tuple)) != 0)
		return rc;
	UNLOCK_INNER_TUPLE (innerElement.tuple);
	
	// Scan of outer tuples that join with inner tuple
	if ((rc = outerSynopsis -> getScan (outerScanId, outerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0)										   
				return rc;

			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}

		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (outerTuple, OUTER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 8 of HR implementation by LAM
		
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_MINUS;
		scanWhenStalled = outerScan;
		stallElement = innerElement;
		LOCK_INNER_TUPLE (stallElement.tuple);
	}
	
	else {
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;
	}
	
	return 0;
}

/**
 * Clear a stall that occurred while processing an outer PLUS element. 
 */

int BinaryJoin::clearStallOuterPlus ()
{
	int rc;
	// Outer element when stalled
	Element outerElement;

	// Inner scan when stalled
	TupleIterator *innerScan;
	
	Tuple innerTuple;
	Tuple outputTuple;	
	Element outputElement;
	
	outerElement = stallElement;
	innerScan = scanWhenStalled;

	// Optimism
	bStalled = false;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (innerTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processOuterMinus & 
		// processInnerMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 9 of HR implementation by LAM

		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_PLUS;
		scanWhenStalled = innerScan;		
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}	
	
	return 0;
}

int BinaryJoin::clearStallOuterMinus ()
{
	int rc;
	Element outerElement;
	TupleIterator *innerScan;
	Tuple innerTuple;
	Tuple outputTuple;
	Element outputElement;
	
	outerElement = stallElement;
	innerScan = scanWhenStalled;

	bStalled = false;
	
	while (!outputQueue -> isFull() && innerScan -> getNext(innerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerElement.tuple;
			lineage [1] = innerTuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0)										   
				return rc;
			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}
		
		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (innerTuple, INNER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = outerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 10 of HR implementation by LAM
		
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = OUTER_MINUS;
		scanWhenStalled = innerScan;
	}
	
	else {
		UNLOCK_OUTER_TUPLE(stallElement.tuple);
		if ((rc = innerSynopsis -> releaseScan (innerScanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;	
}	

int BinaryJoin::clearStallInnerPlus ()
{
	int rc;
	Element innerElement;
	TupleIterator *outerScan;
	Tuple outerTuple;
	Tuple outputTuple;
	Element outputElement;

	innerElement = stallElement;
	outerScan = scanWhenStalled;

	bStalled = false;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// allocate space for the output tuple
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		// construct the output tuple
		evalContext -> bind (outerTuple, OUTER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);		
		outputConstructor -> eval ();
		
		// We have to insert this tuple in joinSynopsis (if it exists) to
		// enable us to produce MINUS tuples (see processInnerMinus & 
		// processOuterMinus)
		
		if (joinSynopsis) {
			
			// Lineage for a tuple is the set of tuples that produced it. 
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> insertTuple (outputTuple, lineage);
			if (rc != 0)									  
				return rc;

			LOCK_OUT_TUPLE (outputTuple);
		}
		
		// construct output element
		outputElement.kind = E_PLUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 11 of HR implementation by LAM
		lastOutputTs = outputElement.timestamp;

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_PLUS;
		scanWhenStalled = outerScan;		
	}
	
	else {
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinaryJoin::clearStallInnerMinus ()
{
	int rc;
	Element innerElement;
	TupleIterator *outerScan;
	Tuple outerTuple;
	Tuple outputTuple;
	Element outputElement;
	
	innerElement = stallElement;
	outerScan = scanWhenStalled;

	bStalled = false;
	
	while (!outputQueue -> isFull() && outerScan -> getNext(outerTuple)) {
		
		// If we are maintaining the joinSynopsis, we can use it to
		// directly retrieve the output tuple.  The output tuple would
		// have been created when the PLUS tuples were joined [[
		// Explanation ]]
		
		if (joinSynopsis) {
			
			lineage [0] = outerTuple;
			lineage [1] = innerElement.tuple;
			
			rc = joinSynopsis -> getTuple (lineage, outputTuple);
			if (rc != 0)										   
				return rc;
			rc = joinSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
		}

		// Otherwise  we  freshly create  a  new  tuple.   Note taht  this
		// violates  the   requirement  that   a  PLUS  element   and  its
		// corresponding MINUS element refer to the same data tuple - this
		// requirement is  needed only when  the tuples are  inserted into
		// synopses upstream.  So the fact that we are here means that
		// there are no synopses upstream for these tuples ...		
		
		else {
			
			// allocate space for the output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0)
				return rc;
			
			// construct the output tuple
			evalContext -> bind (outerTuple, OUTER_ROLE);
			evalContext -> bind (outputTuple, OUTPUT_ROLE);		
			outputConstructor -> eval ();
		}
		
		// output element
		outputElement.kind = E_MINUS;
		outputElement.tuple = outputTuple;
		outputElement.timestamp = innerElement.timestamp;

		outputQueue -> enqueue (outputElement);
		
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 12 of HR implementation by LAM
		lastOutputTs = outputElement.timestamp;		
	}
	
	// We stalled: It is possible that outerScan is also over, in which
	// case we are strictly not stalled, but ignoring this case simplifies
	// code.  We will discover that we are not really stalled later ...
	
	if (outputQueue -> isFull()) {
		bStalled = true;
		stallType = INNER_MINUS;
		scanWhenStalled = outerScan;
	}
	
	else {
		UNLOCK_INNER_TUPLE(stallElement.tuple);
		if ((rc = outerSynopsis -> releaseScan (outerScanId, outerScan)) != 0)
			return rc;		
	}
	
	return 0;
}

int BinaryJoin::clearStall()
{
	if (stallType == OUTER_PLUS)
		return clearStallOuterPlus ();
	
	if (stallType == OUTER_MINUS)
		return clearStallOuterMinus ();

	if (stallType == INNER_PLUS)
		return clearStallInnerPlus ();

	if (stallType == INNER_MINUS)
		return clearStallInnerMinus ();

	// should never reach
	ASSERT (0);

	return -1;
}

//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return bool true or false
 * true when the statistics have changed and false otherwise 
 */ 


bool BinaryJoin::calculateLocalStats() 
{
  // no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0) {
    return false;
  }
  // THIS IS STATISTICS CALCULATION WITH OUTLIER DETECTION
  //calculate local selectivity & cost
  if (!firstRefresh) { 
   
    local_selectivity = (1-ALPHA) * local_selectivity +
      ALPHA * (double) n_tuples_outputted /(double) num_tuples_processed;
    
    double old_local_cost = local_cost_per_tuple;

    double current_avg = (double) local_cost / (double) num_tuples_processed;

    local_cost_per_tuple = (1-ALPHA) * local_cost_per_tuple +
      ALPHA * current_avg; // /time_unit;
    
    k_measurements += 1;
    M_k = M_k_1 + ( current_avg - M_k_1)/k_measurements;
    S_k += ( current_avg - M_k_1 ) * ( current_avg - M_k );
    M_k_1 = M_k;
    
    avg_k = (1-ALPHA) * avg_k +  ALPHA * current_avg; 
    
    stdev = sqrt ( S_k / ( k_measurements -1 ) ) ;
    
    if ( (current_avg - avg_k) > OULIER_FACTOR*stdev) {
      //printf("OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "JOIN OPERATOR %d costs %llu  n_tuples %d smoothed %f selectivity %f\n",
    //     operator_id, local_cost, num_tuples_processed,local_cost_per_tuple,
    //     local_selectivity);

  }
  else {
    if ( num_tuples_processed < 10) {
      return false;
    }
    local_selectivity = (double) n_tuples_outputted / (double) num_tuples_processed;
    
    local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed; // /time_unit;
    M_k = local_cost_per_tuple;
    avg_k = local_cost_per_tuple;
    M_k_1 = M_k;
    S_k = 0;
    k_measurements = 1;
    //printf ( "OPERATOR %d costs %llu  n_tuples %d\n",
    //	     operator_id, local_cost, num_tuples_processed);
  }

  firstRefresh = false;
 
  //load manager, by Thao Pham
  snapshot_local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed;
  //end of load manager, by Thao Pham 
  return true;
}

/** this method is used in order to calculate the priority
 * of the operator. This is used in order to assign the priorities
 * used by the scheduler to schedule operators.
 */

void BinaryJoin::refreshPriority()
{
  mean_cost = local_cost_per_tuple;
  mean_selectivity = 0;
  //if I have not refreshed my stats --> 
  // no priority to refresh
  if ( firstRefresh ) 
    return;  
  //is this operator a shared operator 
  if ( numOutputs != 1 ) {
    mean_selectivity = 0;

    //used for PDT
    priority_queue<Operator*,vector<Operator*>, CompareOPS> mypq;
   
    for ( int i = 0; i< numOutputs; i++ ) {
      //IMPLEMENTING PDT  check sharaf's TODS 2008 paper
      mypq.push(outputs[i]);
      
    }//end of for loop
    double new_priority = 0.0; 
    double old_priority = 0.0;
    double new_mean_selectivity = 0;
    
    double new_mean_cost = local_cost_per_tuple;
    
    for ( int i=0; i < numOutputs; i++ ) {
      Operator * top_queue = mypq.top();
      //printf("top queue %d %f \n " , i, top_queue->priority );
      mypq.pop();
      // if I am outputting tuples to the user
      // along one of my paths
      //--> I should only add up my selectivity
      if ( top_queue->operator_type == OUTPUT ||
	   top_queue->operator_type == SINK ||
	   top_queue->firstRefresh == true){
         new_mean_selectivity += local_selectivity;

	 }
      else if ( !(top_queue->operator_type == BIN_JOIN ||
		  top_queue->operator_type == BIN_STR_JOIN ) ){
	//ADDED THIS BECAUSE IN EXPERIMENTS THE COST OF AN ISTREAM OP
	// EXCEEDED THE COST OF A JOIN OP 
	// TO MAKE HR BETTER WE DECIDED TO IGNORE THE COST OF THE ISTREAM
	if ( top_queue->operator_type != ISTREAM || 
	     ( top_queue->operator_type == ISTREAM && top_queue->mean_cost > 0)) {
	  new_mean_cost += top_queue->mean_cost*local_selectivity;
	  new_mean_selectivity +=  local_selectivity * 
	    top_queue->mean_selectivity;
	}
	// one of my outputs is an istream op
	else {
	  new_mean_selectivity += local_selectivity;
	}
      }	
      //we do not get here unless this operator is a window operator
      // since only window operators output to joins
      else {
	//my output is a join 
	int j = 0; 
	if ( top_queue->inputs[0]->operator_id ==operator_id)
	  j = 1;
	double total_mean_costs = 0;
	for ( int k = 0; k < top_queue->numOutputs; k++)		
	  total_mean_costs+= top_queue->outputs[k]-> mean_cost;
	//check Sharaf's TODS 2008 paper for this formula
	new_mean_cost += local_selectivity * top_queue->local_cost_per_tuple + 
	  ( local_selectivity * top_queue->local_selectivity * 
	    top_queue->inputs[j]->local_selectivity *
	    top_queue->inputs[j]->n_tuples_win *
	    total_mean_costs);
	
	new_mean_selectivity += local_selectivity * top_queue->mean_selectivity
	  * top_queue->inputs[j]->local_selectivity
	  * top_queue->inputs[j]->n_tuples_win;
	
      } //this operator is a join
      new_priority = new_mean_selectivity / new_mean_cost;
      if ( new_priority < old_priority )
	break;
      mean_selectivity = new_mean_selectivity;
      mean_cost = new_mean_cost;
      old_priority = new_priority; 
      
    } // end of for loop
    
  }//end of if the operator is shared
  else {
    // if I am the last operator along the path
    // then my mean selectivity and mean cost
    // are equal to my local ones.
    if ( outputs[0]->operator_type == OUTPUT ||
         outputs[0]->operator_type == SINK ||
	 outputs[0]->firstRefresh == true){
         mean_cost = local_cost_per_tuple;
	 mean_selectivity = local_selectivity; 
	 }

    // is its output a join ?
    else if ( !(outputs[0]->operator_type == BIN_JOIN ||
		outputs[0]->operator_type == BIN_STR_JOIN ) )       
      {
	// ADDED THIS BECAUSE IN EXPERIMENTS THE COST OF AN ISTREAM OP
	// EXCEEDED THE COST OF A JOIN OP 
	// TO MAKE HR BETTER WE DECIDED TO IGNORE THE COST OF THE ISTREAM
	if ( outputs[0]->operator_type != ISTREAM || 
	     ( outputs[0]->operator_type == ISTREAM && outputs[0]->mean_cost > 0)) {
	  mean_selectivity = local_selectivity * 
	    outputs[0]->mean_selectivity;
	
	  mean_cost = outputs[0]->mean_cost * 
	    local_selectivity + mean_cost; 
	}
	// one of my outputs is an istream op followed by an output op
	else {
	  mean_cost = local_cost_per_tuple;
	  mean_selectivity = local_selectivity; 
	}
		
      }	
    //its output is  a join
    else {
      //we do not get here unless this operator is a window operator
      // since only window operators output to joins
      //my output is a join 
      int j = 0; 
      if ( outputs[0]->inputs[0]->operator_id ==operator_id)
	j = 1;
      double total_mean_costs = 0;
      for ( int k = 0; k < outputs[0]->numOutputs; k++)		
	total_mean_costs+=outputs[0]->outputs[k]-> mean_cost;
      //check sharaf's TODS 2008 paper 
      mean_cost = local_cost_per_tuple + 
	local_selectivity * outputs[0]->local_cost_per_tuple  + 
	( local_selectivity * outputs[0]->local_selectivity *
	  outputs[0]->inputs[j]->local_selectivity *
	  outputs[0]->inputs[j]->n_tuples_win *
	  total_mean_costs);
				    
      mean_selectivity = local_selectivity * outputs[0]->mean_selectivity
	* outputs[0]->inputs[j]->local_selectivity
	* outputs[0]->inputs[j]->n_tuples_win;
    } //end of else
  }//end of if not shared
  
  //calculate priority
  priority = mean_selectivity / mean_cost;
  // printf("mean selectivity %f mean_cost %f priority %f\n", 
  // mean_selectivity,  mean_cost, priority );
  
  //reset stats 
  num_tuples_processed= 0;
  n_tuples_outputted = 0;
  local_cost = 0;

}
// end of part 13 of HR implementation by LAM

//HR with ready by Lory Al Moakar 
/** this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int BinaryJoin::readyToExecute() {

  if ( outputQueue->isFull() ) 
    return 0;
  return min ( innerInputQueue->size(), outerInputQueue->size() );
}

// end of part 1 of HR with ready by LAM

int BinaryJoin::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}

/////////////////////////
//ArmaDiLos, by Thao Pham
void BinaryJoin::deactivate(){

	status = INACTIVE;

	bStalled = false;
	Element e;
	while(!innerInputQueue->isEmpty()){
		innerInputQueue->dequeue(e);
		UNLOCK_INNER_TUPLE(e.tuple);
	}
	while(!outerInputQueue->isEmpty()){
		outerInputQueue->dequeue(e);
		UNLOCK_OUTER_TUPLE(e.tuple);
	}
	Tuple t;
	//delete the synopsis and decref of the tuples (in stores) pointed to by the synopsis entries
	if(innerSynopsis)
		innerSynopsis->clearSyn(innerInputStore);
	if(outerSynopsis)
		outerSynopsis->clearSyn(outerInputStore);

	//clear join synopsis as well
	if(joinSynopsis)
		joinSynopsis->clearSyn(outStore);

	if(outputQueue->isEmpty()){
		for(int i=0;i<numOutputs;i++){
			outputs[i]->deactivate();
		}
	}

	resetLocalStatisticsComputationCycle();
}
//end of ArmaDiLos


