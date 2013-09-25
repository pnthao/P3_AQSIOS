/**
 * @file         group_aggr.cc
 * @date         May 30, 2004
 * @brief        Group by aggregation operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _GROUP_AGGR_
#include "execution/operators/group_aggr.h"
#endif
//HR implementation by Lory Al Moakar
#ifndef _COMPAREOPS_
#include "execution/operators/compareOPS.h"
#endif

#include <queue>
#include <math.h>
//end of part 0 of HR implementation by LAM

#define LOCK_OUTPUT_TUPLE(t)   (outStore -> addRef ((t)))
#define LOCK_INPUT_TUPLE(t)    (inStore -> addRef ((t)))
#define UNLOCK_OUTPUT_TUPLE(t) (outStore -> decrRef ((t)))
#define UNLOCK_INPUT_TUPLE(t)  (inStore -> decrRef ((t)))


using namespace Execution;

GroupAggr::GroupAggr (unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                  = id;
	this -> inputQueue          = 0;
	this -> outputQueue         = 0;
	this -> inputSynopsis       = 0;
	this -> outputSynopsis      = 0;
	this -> outStore            = 0;
	this -> inStore             = 0;
	this -> evalContext         = 0;
	this -> plusEval            = 0;
	this -> updateEval          = 0;
	this -> minusEval           = 0;
	this -> bScanNotReq         = 0;
	this -> initEval            = 0;
	this -> emptyGroupEval      = 0;
	this -> lastInputTs         = 0;
	this -> lastOutputTs        = 0;
	this -> bStalled            = false;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = GROUP_AGGR;
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
	//end of load manager, by Thao Pham
}

GroupAggr::~GroupAggr ()
{
	if (evalContext)
		delete evalContext;
	if (plusEval)
		delete plusEval;
	if (updateEval)
		delete updateEval;
	if (bScanNotReq)
		delete bScanNotReq;
	if (minusEval)
		delete minusEval;
	if (initEval)
		delete initEval;
	if (emptyGroupEval)
		delete emptyGroupEval;	
}

int GroupAggr::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int GroupAggr::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int GroupAggr::setOutputSynopsis (RelationSynopsis *synopsis,
								  unsigned int scanId)
{
	ASSERT (synopsis);
	
	this -> outputSynopsis = synopsis;
	this -> outScanId = scanId;
	return 0;
}

int GroupAggr::setInputSynopsis (RelationSynopsis *synopsis,
								 unsigned int scanId)
{
	this -> inputSynopsis = synopsis;
	this -> inScanId = scanId;
	return 0;
}

int GroupAggr::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int GroupAggr::setInStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> inStore = store;
	return 0;
}

int GroupAggr::setPlusEvaluator (AEval *plusEval)
{
	ASSERT (plusEval);
	
	this -> plusEval = plusEval;
	return 0;
}

int GroupAggr::setMinusEvaluator (AEval *minusEval)
{
	this -> minusEval = minusEval;
	return 0;
}

int GroupAggr::setEmptyGroupEvaluator (BEval *emptyGroupEval)
{
	this -> emptyGroupEval = emptyGroupEval;
	return 0;
}

int GroupAggr::setInitEvaluator (AEval *initEval)
{
	ASSERT (initEval);
	
	this -> initEval = initEval;
	return 0;
}

int GroupAggr::setUpdateEvaluator (AEval *updateEval)
{
	this -> updateEval = updateEval;
	return 0;
}

int GroupAggr::setRescanEvaluator (BEval *eval)
{
	this -> bScanNotReq = eval;
	return 0;
}

int GroupAggr::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int GroupAggr::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							
 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM
	
	// We have a stall and cannot clear it. Note the use of short-circuit
	// evaluation ..
	if (bStalled) {
		if (!outputQueue -> enqueue (stalledElement)) {
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif										
			return 0;
		}
		
		bStalled = false;
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	}
	
	numElements = timeSlice;
	
	unsigned int e = 0 ;
	for ( ; (e < numElements) && !bStalled ; e++) {
		
		// No space in output queue -- no scope for any processing
		if (outputQueue -> isFull())
			break;
		
		// Get the next input element
		if (!inputQueue -> dequeue (inputElement)){
		 
		  break;
		}
		
		lastInputTs = inputElement.timestamp;
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM
		
		// Heartbeats can be ignored
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		if (inputElement.kind == E_PLUS) {
			rc = processPlus (inputElement);
			if (rc != 0) return rc;
		}
		
		else {
			ASSERT (inputElement.kind == E_MINUS);
			rc = processMinus (inputElement);
			if (rc != 0) return rc;
		}
		
		UNLOCK_INPUT_TUPLE (inputElement.tuple);

		
	}
	
	// process heartbeats
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat (lastInputTs));
		lastOutputTs = lastInputTs;
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
	


	

	//end of part 4 of HR implementation by LAM
		 
	return 0;
}

int GroupAggr::processPlus (Element inputElement)
{
	int rc;
	Tuple          inpTuple;        // input tuple
	Tuple          oldAggrTuple;    // existing aggr for the tuple's group
	Tuple          newAggrTuple;    // new aggr for the tuple's group
	TupleIterator *scan;
	bool           bGroupExists;
	Element        plusElement, minusElement;
	
	ASSERT (!bStalled);
	
	inpTuple = inputElement.tuple;
	
	// Bind the input tuple
	evalContext -> bind (inpTuple, INPUT_ROLE);
	
	// Get a scan that produces the group's aggregation tuple if it
	// exists. 
	if ((rc = outputSynopsis -> getScan (outScanId, scan)) != 0)
		return rc;
	
	bGroupExists = scan -> getNext (oldAggrTuple);
	
#ifdef _DM_
	// There should be only one aggregation tuple per group
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif

	if ((rc = outputSynopsis -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	// We have seen a tuple belonging to this group before
	if (bGroupExists) {
		
		// We will create a new aggregation tuple for this group.
		// We should not overwrite the oldAggrTuple since someone
		// downstream might be reading it ...
		rc = outStore -> newTuple (newAggrTuple);
		if (rc != 0) return rc;		
		
		// Plus evaluator does the job of computing the new aggregation
		// tuple from  the new input tuple and the old aggregation tuple.
		evalContext -> bind (oldAggrTuple, OLD_OUTPUT_ROLE);
		evalContext -> bind (newAggrTuple, NEW_OUTPUT_ROLE);
		plusEval -> eval ();
		
		// We insert the new aggr. tuple into the synopsis & delete the
		// old one 
		rc = outputSynopsis -> insertTuple (newAggrTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (newAggrTuple);
		
		rc = outputSynopsis -> deleteTuple (oldAggrTuple);
		if (rc != 0) return rc;		
		
		// Send a plus element corr. to newAggrTuple, and a minus element
		// corr. to oldAggrTuple.  We will never be blocked on the first
		// enqueue, since we have ensured that the queue is non-full
		// before coming here.
		ASSERT (!outputQueue -> isFull());
		
		// Plus element:
		plusElement.kind      = E_PLUS;
		plusElement.tuple     = newAggrTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
		lastOutputTs = inputElement.timestamp;
		
		// Minus element:
		minusElement.kind      = E_MINUS;
		minusElement.tuple     = oldAggrTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We could get stalled now though ...
		if (!outputQueue -> enqueue (minusElement)) {			
			bStalled = true;
			stalledElement = minusElement;
		}		
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		if ( !bStalled )
		  n_tuples_outputted +=  1;
		// end of part 6 of HR implementation by LAM
	}
	
	// This is the first tuple belonging to this group
	else {
		// We create a new aggregation tuple for this new group.
		rc = outStore -> newTuple (newAggrTuple);
		if (rc != 0) return rc;
		
		evalContext -> bind (newAggrTuple, NEW_OUTPUT_ROLE);
		initEval -> eval ();
		
		// Insert the new aggregation tuple into the synopsis
		rc = outputSynopsis -> insertTuple (newAggrTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (newAggrTuple);
		
		// Send a plus element corresponding to this tuple. We will never
		// be blocked, since we have ensured that the queue is non-full
		// before coming here.
		ASSERT (!outputQueue -> isFull());
		
		plusElement.kind      = E_PLUS;
		plusElement.tuple     = newAggrTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 7 of HR implementation by LAM
		lastOutputTs = inputElement.timestamp;
	}
	
	// Maintain the input synopsis if it exists
	if (inputSynopsis) {
		rc = inputSynopsis -> insertTuple (inpTuple);
		if (rc != 0) return rc;
		
		LOCK_INPUT_TUPLE (inpTuple);
	}
	
	return 0;
}

int GroupAggr::processMinus (Element inputElement)
{
	int rc;
	Tuple          inpTuple;        // input tuple
	Tuple          oldAggrTuple;    // existing aggr for the tuple's group
	Tuple          newAggrTuple;    // new aggr for the tuple's group
	TupleIterator *scan;
	bool           bGroupExists;
	Element        plusElement, minusElement;
	
	ASSERT (!bStalled);
	
	inpTuple = inputElement.tuple;

	evalContext -> bind (inpTuple, INPUT_ROLE);

	// Maintain the input synopsis if it exists.  This should be done
	// before the rest of the processing since later code assumes that the
	// input synopsis is up-to-date for its correctness.
	if (inputSynopsis) {
		rc = inputSynopsis -> deleteTuple (inpTuple);
		if (rc != 0) return rc;

		UNLOCK_INPUT_TUPLE (inpTuple);
	}
	
	// Perform a scan to locate the current aggregation tuple for
	// the group to which the inpTuple belongs to, if it exists
	rc = outputSynopsis -> getScan (outScanId, scan);
	if (rc != 0) return rc;
	
	// Since we have got a MINUS tuple now, the PLUS tuple for it should
	// have arrived earlier, implying that there should be an output entry
	// for this group.
	bGroupExists = scan -> getNext (oldAggrTuple);	
	ASSERT (bGroupExists);
	
	// There should be only one aggregation tuple per group
#ifdef _DM_
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif
	rc = outputSynopsis -> releaseScan (outScanId, scan);
	if (rc != 0) return rc;
	
	evalContext -> bind (oldAggrTuple, OLD_OUTPUT_ROLE);
	
	// There are two possibilities: the group becomes empty after this
	// minus tupls is processed, or otherwise.  emptyGroupEval (a slight
	// misnomer) checks if there is only one element in this group.		
	if (emptyGroupEval -> eval()) {
		
		// delete the old aggregation tuple from our synopsis
		rc = outputSynopsis -> deleteTuple (oldAggrTuple);
		if (rc != 0) return rc;
		
		// Minus element:
		minusElement.kind      = E_MINUS;
		minusElement.tuple     = oldAggrTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We have ensured before coming here that the output queue is
		// nonfull 
		ASSERT (!outputQueue -> isFull());
		outputQueue -> enqueue (minusElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 8 of HR implementation by LAM		


		lastOutputTs = minusElement.timestamp;
	}
	
	// The group remains active.  
	else {					
		
		// We will create a new aggregation tuple for this group.
		// We should not overwrite the oldAggrTuple since someone
		// downstream might be reading it ...
		rc = outStore -> newTuple (newAggrTuple);
		if (rc != 0) return rc;
		
		// Produce the new aggr. tuple for the group.
		rc = produceOutputTupleForMinus (newAggrTuple);
		if (rc != 0) return rc;
		
		// Insert the new aggr. tuple into the output synopsis and delete
		// the old one
		rc = outputSynopsis -> insertTuple (newAggrTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (newAggrTuple);
		
		rc = outputSynopsis -> deleteTuple (oldAggrTuple);
		if (rc != 0) return rc;				
		
		// Send a plus element corr. to newAggrTuple, and a minus element
		// corr. to oldAggrTuple.  We will never be blocked on the first
		// enqueue, since we have ensured that the queue is non-full
		// before coming here.
		ASSERT (!outputQueue -> isFull());
		
		// Plus element:
		plusElement.kind      = E_PLUS;
		plusElement.tuple     = newAggrTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 9 of HR implementation by LAM
		lastOutputTs = inputElement.timestamp;
		
		// Minus element:
		minusElement.kind      = E_MINUS;
		minusElement.tuple     = oldAggrTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We could get stalled now though ...
		if (!outputQueue -> enqueue (minusElement)) {			
			bStalled = true;
			stalledElement = minusElement;
		}
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		if ( !bStalled )
		  n_tuples_outputted +=  1;
		// end of part 10 of HR implementation by LAM
		
	}
	
	return 0;
}

int GroupAggr::produceOutputTupleForMinus (Tuple newAggrTuple)
{
	int rc;
	TupleIterator *inScan;
	Tuple inTuple;
	bool bNotEmpty;
	
	evalContext -> bind (newAggrTuple, NEW_OUTPUT_ROLE);
	
	// Assert: At this point, evalContext contains the new input tuple
	// (MINUS) and the old aggr. tuple for the input tuples group bound.
	
	// We first determine if we need to scan the entire inner relation to
	// update the new aggregation tuple (this can happen if we have MAX or
	// MIN aggregation functions) or if we can incrementatlly produce the
	// new Aggr. tuple for the group from the old aggr. tuple and the new
	// input tuple.
	
	// Scan required: We will essentially redo the steps that we used for
	// computing the aggr. tuple for the group in response to PLUS tuples.
	if (bScanNotReq -> eval ()) {		
		minusEval -> eval ();
	}

	else {
		// scan iterator that returns all tuples in input synopsis
		// corresponding to the present group.
		if ((rc = inputSynopsis -> getScan (inScanId, inScan)) != 0)
			return rc;

		// If we come here the synopsis can't be empty
		bNotEmpty = inScan -> getNext (inTuple);
		ASSERT (bNotEmpty);
		
		// initialize newAggrTuple
		evalContext -> bind (inTuple, INPUT_ROLE);
		initEval -> eval ();
		
		// Inplace update of newAggrTuple
		while (inScan -> getNext (inTuple)) {
			evalContext -> bind (inTuple, INPUT_ROLE);
			updateEval -> eval ();
		}
		
		if ((rc = inputSynopsis -> releaseScan (inScanId, inScan)) != 0)
			return rc;
	}
	
	return 0;
}
	
//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return bool true or false
 * true when the statistics have changed and false otherwise 
 */ 
bool GroupAggr::calculateLocalStats() 
{
  // no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0) {
    return false;
  }
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
    
    //printf ( "GROUP_AGGR OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
    //     operator_id, local_cost, num_tuples_processed,local_cost_per_tuple);

  }
  else {
   
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

void GroupAggr::refreshPriority()
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
    std::priority_queue <Operator*,std::vector<Operator*>, CompareOPS> mypq;

    for ( int i = 0; i< numOutputs; i++ ) {
      //IMPLEMENTING PDT 
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
      else if (!( top_queue->operator_type == BIN_JOIN ||
		 top_queue->operator_type == BIN_STR_JOIN)){
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
	if ( top_queue->inputs[0]->operator_id == operator_id)
	  j = 1;
	double total_mean_costs = 0;
	for ( int k = 0; k < top_queue->numOutputs; k++)		
	  total_mean_costs+= top_queue->outputs[k]-> mean_cost;
	//check sharaf's TODS 2008 paper for formula
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
		outputs[0]->operator_type == BIN_STR_JOIN) )       
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
      //check sharaf's TODS 2008 paper for formula
      mean_cost = local_cost_per_tuple + 
	local_selectivity * outputs[0]->local_cost_per_tuple  + 
	( local_selectivity * outputs[0]->local_selectivity *
	  outputs[0]->inputs[j]->local_selectivity *
	  outputs[0]->inputs[j]->n_tuples_win *
	  total_mean_costs);
				    
      mean_selectivity = local_selectivity *outputs[0]->mean_selectivity
	* outputs[0]->inputs[j]->local_selectivity
	* outputs[0]->inputs[j]->n_tuples_win;
    } //end of else
  }//end of if not shared
  
  //calculate priority
  priority = mean_selectivity / mean_cost;

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
int GroupAggr::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return inputQueue->size();
}

// end of part 1 of HR with ready by LAM
int GroupAggr::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
