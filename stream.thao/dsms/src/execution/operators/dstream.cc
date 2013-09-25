/**
 * @file       dstream.cc
 * @date       Sep. 7, 2004
 * @brief      Implementation of Dstream operator
 */

#ifndef _DSTREAM_
#include "execution/operators/dstream.h"
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
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))
#define UNLOCK_SYN_TUPLE(t)   (synStore -> decrRef ((t)))

using namespace std;
using namespace Execution;

Dstream::Dstream (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> inputQueue = 0;
	this -> outputQueue = 0;
	this -> synopsis = 0;
	this -> synStore = 0;
	this -> outStore = 0;
	this -> inStore = 0;
	this -> evalContext = 0;
	this -> incrEval = 0;
	this -> initEval = 0;
	this -> decrEval = 0;
	this -> zeroEval = 0;
	this -> outEval = 0;
	this -> lastInputTs = 0;
	this -> lastOutputTs = 0;
	this -> curTs = 0;
	this -> scanWhenStalled = 0;
	this -> bStalled = false;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = DSTREAM;
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

Dstream::~Dstream () {}

int Dstream::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int Dstream::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int Dstream::setSynopsis (RelationSynopsis *syn)
{
	ASSERT (syn);

	this -> synopsis = syn;
	return 0;
}

int Dstream::setCountScan (unsigned int scanId)
{
	this -> countScanId = scanId;
	return 0;
}

int Dstream::setFullScan (unsigned int scanId)
{
	this -> fullScanId = scanId;
	return 0;
}

int Dstream::setSynStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> synStore = store;
	return 0;
}
		
int Dstream::setOutStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> outStore = store;
	return 0;
}

int Dstream::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Dstream::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int Dstream::setIncrEval (AEval *eval)
{
	ASSERT (eval);

	this -> incrEval = eval;
	return 0;
}

int Dstream::setDecrEval (AEval *eval)
{
	ASSERT (eval);

	this -> decrEval = eval;
	return 0;
}

int Dstream::setInitEval (AEval *eval)
{
	ASSERT (eval);

	this -> initEval = eval;
	return 0;
}

int Dstream::setZeroEval (BEval *eval)
{
	ASSERT (eval);

	this -> zeroEval = eval;
	return 0;
}

int Dstream::setNegEval (BEval *eval)
{
	ASSERT (eval);

	this -> negEval = eval;
	return 0;
}

int Dstream::setOutEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> outEval = eval;
	return 0;
}

int Dstream::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							

 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM
	
	// number of elements we will process before returning
	numElements = timeSlice;

	// We are stalled, need to clear it first
	if (bStalled && ((rc = clearStall()) != 0))
		return rc;
	
	// We did not succeed in clearing the stall, return without
	// further processing
	if (bStalled) {
#ifdef _MONITOR_
		stopTimer ();
		logOutTs (lastOutputTs);
#endif									
		return 0;
	}
	unsigned int e = 0 ;
	
	for ( ; e < numElements ; e++) {
		
	        // Get the next input element
	        if (!inputQueue -> dequeue (inputElement)) {
		  break;
		}
		
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM
		 
		lastInputTs = inputElement.timestamp;
		
		// If the timestamp of the input element > curTs, we can produce
		// the output (data) elements with timestamp = curTs.
		if (curTs < lastInputTs) {
			if ((rc = produceOutput ()) != 0)
				return rc;
			
			// We stalled while producing output:
			if (bStalled) {
				stallElement = inputElement;
				break;
			}
		}
		
		// Process a plus tuple
		if (inputElement.kind == E_PLUS) {			
			rc = handlePlus (inputElement);
			if (rc != 0) return rc;

			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}
		
		// Process a minus tuple
		else if (inputElement.kind == E_MINUS) {			
			rc = handleMinus (inputElement);
			if (rc != 0) return rc;

			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}
		
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

int Dstream::produceOutput ()
{
	int rc;
	TupleIterator *scan;
	Tuple synTuple;
	Tuple outTuple;
	Element outElement;

	ASSERT (!bStalled);
	
	if ((rc = synopsis -> getScan (fullScanId, scan)) != 0)
		return rc;
	
	while (scan -> getNext (synTuple)) {
		
		evalContext -> bind (synTuple, SYN_ROLE);
		
		// If the count of a synTuple is k, we produce k elements on the
		// output, each of whose tuple attribute is a copy of synTuple,
		// timestamp = curTs.
		while (!outputQueue -> isFull() && negEval -> eval()) {
			
			// Allocate space for a new output tuple
			if ((rc = outStore -> newTuple (outTuple)) != 0)
				return rc;
			
			// copy non-count fields of synTuple to outTuple
			evalContext -> bind (outTuple, OUTPUT_ROLE);			
			outEval -> eval();
			
			// generate the output element & enqueue it
			outElement.kind = E_PLUS;
			outElement.tuple = outTuple;
			outElement.timestamp = curTs;
			
			outputQueue -> enqueue (outElement);

			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 5 of HR implementation by LAM
			
			lastOutputTs = curTs;
			
			// decrement the count of the tuple
			incrEval -> eval();
		}
		
		// If the output queue is full, we take this as a stall.  We
		// remember the synTuple, since we might have to generate
		// one or more copies of this tuple in the output later.
		if (outputQueue -> isFull()) {
			bStalled = true;
			synTupleWhenStalled = synTuple;
			scanWhenStalled = scan;
			
			return 0;
		}
		
		// The output queue is not full ==> we have generated the correct
		// no of copies (possibly 0) of synTuple in the output.  We delete
		// the synTuple from the input synopsis.  Note that the synopsis
		// guarantees that the scan continues to be valid even after this
		// deletion.
		if ((rc = synopsis -> deleteTuple (synTuple)) != 0)
			return rc;
		UNLOCK_SYN_TUPLE (synTuple);
	}

	if ((rc = synopsis -> releaseScan (fullScanId, scan)) != 0)
		return rc;
	
	// Update the current timestamp.
	curTs = lastInputTs;
	
	return 0;
}

int Dstream::clearStall ()
{
	int rc;
	Tuple synTuple;
	Tuple outTuple;
	Element outElement;
	TupleIterator *scan;
	
	ASSERT (bStalled && scanWhenStalled && synTupleWhenStalled);
	
	// First, we generate the required number of copies of the synTuple.
	synTuple = synTupleWhenStalled;
	evalContext -> bind (synTuple, SYN_ROLE);
	
	while (!outputQueue -> isFull() && negEval -> eval()) {
		
		// Allocate space for a new output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		// copy non-count fields of synTuple to outTuple
		evalContext -> bind (outTuple, OUTPUT_ROLE);			
		outEval -> eval();
		
		// generate the output element & enqueue it
		outElement.kind = E_PLUS;
		outElement.tuple = outTuple;
		outElement.timestamp = curTs;
		
		outputQueue -> enqueue (outElement);

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 6 of HR implementation by LAM
		
		lastOutputTs = curTs;
		
		// decrement the count of the tuple
		decrEval -> eval();
	}
	
	// If the output queue is full, we have failed to clear the stall.
	if (outputQueue -> isFull()) 
		return 0;
	
	// We managed to generate the required number of output tuples.
	// We delete the synTuple from our synopsis
	if ((rc = synopsis -> deleteTuple (synTuple)) != 0)
		return rc;
	UNLOCK_SYN_TUPLE (synTuple);

	scan = scanWhenStalled;
	while (scan -> getNext (synTuple)) {
		
		evalContext -> bind (synTuple, SYN_ROLE);
		
		// If the count of a synTuple is k, we produce k elements on the
		// output, each of whose tuple attribute is a copy of synTuple,
		// timestamp = curTs.
		while (!outputQueue -> isFull() && negEval -> eval()) {
			
			// Allocate space for a new output tuple
			if ((rc = outStore -> newTuple (outTuple)) != 0)
				return rc;
			
			// copy non-count fields of synTuple to outTuple
			evalContext -> bind (outTuple, OUTPUT_ROLE);			
			outEval -> eval();
			
			// generate the output element & enqueue it
			outElement.kind = E_PLUS;
			outElement.tuple = outTuple;
			outElement.timestamp = curTs;
			
			outputQueue -> enqueue (outElement);

			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 5 of HR implementation by LAM

			lastOutputTs = curTs;
			
			// decrement the count of the tuple
			incrEval -> eval();
		}
		
		// We stalled again: remember synTuple & return
		if (outputQueue -> isFull()) {
			synTupleWhenStalled = synTuple;			
			return 0;
		}
		
		// We managed to generate the required no of output tuples.
		// Delete the synTuple from the synopsis
		if ((rc = synopsis -> deleteTuple (synTuple)) != 0)
			return rc;
		UNLOCK_SYN_TUPLE (synTuple);
	}

	if ((rc = synopsis -> releaseScan (fullScanId, scan)) != 0)
		return rc;
	bStalled = false;	
	
	// Update the current timestamp.
	curTs = lastInputTs;

	// Process a plus tuple
	if (stallElement.kind == E_PLUS) {
		
		rc = handlePlus (stallElement);
		if (rc != 0) return rc;
		
		UNLOCK_INPUT_TUPLE (stallElement.tuple);
	}
	
	// Process a minus tuple
	else if (stallElement.kind == E_MINUS) {			
		rc = handleMinus (stallElement);
		if (rc != 0) return rc;
		
		UNLOCK_INPUT_TUPLE (stallElement.tuple);
	}	
	
	return 0;
}

int Dstream::handlePlus (Element inputElement)
{
	int rc;
	Tuple inTuple;
	TupleIterator *scan;
	Tuple countTuple;
	
	inTuple = inputElement.tuple;
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	// If we have seen a copy of inTuple before, we add 1 to the count
	// corresponding to inTuple.  Otherwise, we initialize the count
	// corresponding to inTuple to 1

	// Scan
	if ((rc = synopsis -> getScan (countScanId, scan)) != 0)
		return rc;

	// We have seen inTuple before
	if (scan -> getNext (countTuple)) {
		
#ifdef _DM_
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));
#endif
		
		evalContext -> bind (countTuple, SYN_ROLE);
		
#ifdef _DM_
		// count should be non-zero
		ASSERT (!zeroEval -> eval());
#endif

		// increment the count
		incrEval -> eval ();

		// If the count becomes zero, we delete the tuple
		if (zeroEval -> eval ()) {
			if ((rc = synopsis -> deleteTuple (countTuple)) != 0)
				return rc;
			UNLOCK_SYN_TUPLE (countTuple);
		}
	}

	// We have not seen inTuple before
	else {
		
		// create a new countTuple
		if ((rc = synStore -> newTuple (countTuple)) != 0)
			return rc;

		// initialize the count to 1
		evalContext -> bind (countTuple, SYN_ROLE);
		initEval -> eval();
		incrEval -> eval();
		
		// insert the tuple to synopsis
		if ((rc = synopsis -> insertTuple (countTuple)) != 0)
			return rc;
	}
	
	if ((rc = synopsis -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}

int Dstream::handleMinus (Element inputElement)
{
	int rc;
	Tuple inTuple;
	TupleIterator *scan;
	Tuple countTuple;
	
	inTuple = inputElement.tuple;
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	// If we have seen a copy of inTuple before, we subtract 1 from the count
	// corresponding to inTuple.  Otherwise, we initialize the count
	// corresponding to inTuple to -1
	
	// Scan
	if ((rc = synopsis -> getScan (countScanId, scan)) != 0)
		return rc;

	// We have seen inTuple before
	if (scan -> getNext (countTuple)) {
		
#ifdef _DM_
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));
#endif
		
		evalContext -> bind (countTuple, SYN_ROLE);
		
#ifdef _DM_
		// count should be non-zero
		ASSERT (!zeroEval -> eval());
#endif

		// decrement the count
		decrEval -> eval ();

		// If the count becomes zero, we delete the tuple
		if (zeroEval -> eval ()) {
			if ((rc = synopsis -> deleteTuple (countTuple)) != 0)
				return rc;
			UNLOCK_SYN_TUPLE (countTuple);
		}
	}
	
	// We have not seen inTuple before
	else {
		
		// create a new countTuple
		if ((rc = synStore -> newTuple (countTuple)) != 0)
			return rc;
		
		// initialize the count to -1
		evalContext -> bind (countTuple, SYN_ROLE);
		initEval -> eval();
		decrEval -> eval();
		
		// insert the tuple to synopsis
		if ((rc = synopsis -> insertTuple (countTuple)) != 0)
			return rc;
	}
	
	if ((rc = synopsis -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}

//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return bool true or false
 * true when the statistics have changed and false otherwise
 */ 
bool Dstream::calculateLocalStats() 
{
  // no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0){
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
      //    old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "DSTREAM OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
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

void Dstream::refreshPriority()
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
      else if ( !( top_queue->operator_type == BIN_JOIN ||
		 top_queue->operator_type == BIN_STR_JOIN ) ){
	new_mean_cost += top_queue->mean_cost*local_selectivity;
	new_mean_selectivity +=  local_selectivity * 
	  top_queue->mean_selectivity;
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
	 outputs[0]->firstRefresh == true ){
         mean_cost = local_cost_per_tuple;
	 mean_selectivity = local_selectivity; 
	 }

    // is its output a join ?
    else if ( !(outputs[0]->operator_type == BIN_JOIN ||
	       outputs[0]->operator_type == BIN_STR_JOIN ))       
      {
	mean_selectivity = local_selectivity * 
	  outputs[0]->mean_selectivity;
	
	mean_cost = outputs[0]->mean_cost * 
	  local_selectivity + mean_cost; 
	
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
int Dstream::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return inputQueue->size();
}

// end of part 1 of HR with ready by LAM

int Dstream::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
