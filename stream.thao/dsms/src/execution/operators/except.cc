#ifndef _EXCEPT_
#include "execution/operators/except.h"
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

using namespace Execution;
using namespace std;

#define MIN(a,b) (((a) < (b))? (a) : (b))

#define UNLOCK_COUNT_TUPLE(t) (countStore -> decrRef((t)))
#define UNLOCK_LEFT_TUPLE(t) (leftInStore -> decrRef ((t)))
#define UNLOCK_RIGHT_TUPLE(t) (rightInStore -> decrRef ((t)))
#define LOCK_OUT_TUPLE(t) (outStore -> addRef ((t)))

Except::Except (unsigned int id, ostream& _LOG)
	: LOG (_LOG)
{
	this -> id             = id;
	this -> leftInQueue    = 0;
	this -> rightInQueue   = 0;
	this -> outputQueue    = 0;
	this -> outStore       = 0;
	this -> outSyn         = 0;
	this -> leftInStore    = 0;
	this -> rightInStore   = 0;
	this -> countSyn       = 0;
	this -> countStore     = 0;
	this -> evalContext    = 0;
	this -> outEval        = 0;
	this -> initEval       = 0;
	this -> cplsEval       = 0;
	this -> cprsEval       = 0;
	this -> lastLeftTs     = 0;
	this -> lastRightTs    = 0;
	this -> lastOutputTs   = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = EXCEPT;
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

Except::~Except () {}

int Except::setRightInputQueue (Queue *queue)
{
	ASSERT (queue);
	
	this -> rightInQueue = queue;
	return 0;
}

int Except::setLeftInputQueue (Queue *queue)
{
	ASSERT (queue);
	
	this -> leftInQueue = queue;
	return 0;
}

int Except::setOutputQueue (Queue *queue)
{
	ASSERT (queue);

	this -> outputQueue = queue;
	return 0;
}

int Except::setOutStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> outStore = store;
	return 0;
}

int Except::setLeftInputStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> leftInStore = store;
	return 0;
}

int Except::setRightInputStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> rightInStore = store;
	return 0;
}

int Except::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);

	this -> evalContext = evalContext;
	return 0;
}

int Except::setCountSyn (RelationSynopsis *syn)
{
	ASSERT (syn);
	
	this -> countSyn = syn;
	return 0;
}

int Except::setCountScanId (unsigned int scanId)
{
	this -> countScanId = scanId;
	return 0;
}

int Except::setCountStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> countStore = store;
	return 0;
}

int Except::setOutSyn (RelationSynopsis *outSyn)
{
	ASSERT (outSyn);

	this -> outSyn = outSyn;
	return 0;
}

int Except::setOutScanId (unsigned int scanId)
{
	this -> outScanId = scanId;
	return 0;
}

int Except::setOutEval (AEval *eval)
{
	ASSERT (eval);

	this -> outEval = eval;
	return 0;
}

int Except::setInitEval (AEval *eval)
{
	ASSERT (eval);

	this -> initEval = eval;
	return 0;
}

int Except::setCopyLeftToScratchEval (AEval *eval)
{
	ASSERT (eval);

	this -> cplsEval = eval;
	return 0;
}

int Except::setCopyRightToScratchEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> cprsEval = eval;
	return 0;
}

int Except::setCountCol (Column col)
{
	this -> countCol = col;
	return 0;
}

int Except::run (TimeSlice timeSlice)
{
	int          rc;
	unsigned int numElements;
	Timestamp    leftMinTs;
	Timestamp    rightMinTs;
	Element      inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif

 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM

	
	// Minimum timestamp possible on the next outer element
	leftMinTs = lastLeftTs;
	
	// Minimum timestamp possible on the next inner element
	rightMinTs = lastRightTs;	
	
	numElements = timeSlice;
	unsigned int e = 0 ;
	
	for ( ; e < numElements ; e++) {

		// Stalled @ output
		if (outputQueue -> isFull ())
			break;
		
		// Minimum possible timestamp for the next left element
		if (leftInQueue -> peek (inputElement))
			leftMinTs = inputElement.timestamp;
		
		// Minimum possible timestamp for the next outer element
		if (rightInQueue -> peek (inputElement))
			rightMinTs = inputElement.timestamp;
		
		// We have to process left input if leftMinTs < rightMinTs.  If
		// leftMinTs = rightMinTs, we have the option of doing so.
		if (leftMinTs <= rightMinTs &&
			leftInQueue -> dequeue (inputElement)) {

		        //HR implementation by Lory Al Moakar
		        // find out the num_tuples_processed so far
		        num_tuples_processed +=  1;
			// end of part 3 of HR implementation by LAM

			lastLeftTs = inputElement.timestamp;
			
			// Process PLUS
			if (inputElement.kind == E_PLUS) {
				if ((rc = handleLeftPlus (inputElement)) != 0)
					return rc;
				
				UNLOCK_LEFT_TUPLE (inputElement.tuple);
	

			}
			
			// Process MINUS
			else if (inputElement.kind == E_MINUS) {
				if ((rc = handleLeftMinus (inputElement)) != 0)
					return rc;
				
				UNLOCK_LEFT_TUPLE (inputElement.tuple);			
			}
			
			// else: ignore heartbeats
		}
		
		// Symmetric argument for right input
		else if (rightMinTs <= leftMinTs &&
				 rightInQueue -> dequeue (inputElement)) {
		        
		        //HR implementation by Lory Al Moakar
		        // find out the num_tuples_processed so far
		        num_tuples_processed +=  1;
			// end of part 3 of HR implementation by LAM
                        
			lastRightTs = inputElement.timestamp;
			
			if (inputElement.kind == E_PLUS) {
				if ((rc = handleRightPlus (inputElement)) != 0)
					return rc;
				
				UNLOCK_RIGHT_TUPLE (inputElement.tuple);
				
			}
			
			// Process MINUS
			else if (inputElement.kind == E_MINUS) {
				if ((rc = handleRightMinus (inputElement)) != 0)
					return rc;
				
				UNLOCK_RIGHT_TUPLE (inputElement.tuple);			
			}
			
			// else: ignore heartbeats
		}
		
		else {
		  break;
		}

	}
	
	// Heartbeat generation
	if ((!outputQueue -> isFull())   &&
		(lastOutputTs < leftMinTs)  &&
		(lastOutputTs < rightMinTs)) {
		
		lastOutputTs = MIN(leftMinTs, rightMinTs);
		
		outputQueue -> enqueue (Element::Heartbeat(lastOutputTs));
		
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

int Except::handleLeftPlus (Element inputElement)
{
	int      rc;
	Tuple    inTuple;
	Tuple    countTuple;
	Tuple    outTuple;
	Element  outElement;
	int      count;
	
	// Input tuple
	inTuple = inputElement.tuple;

	// Get the count tuple for this input tuple
	if ((rc = getCountTuple_l (inTuple, countTuple)) != 0)
		return rc;
	
	// Increment the count
	count = ++ICOL(countTuple, countCol);
	
	// Count is positive: send out an output
	if (count > 0) {
		
		// alloc output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		// copy attributes from count tuple -> outTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		evalContext -> bind (outTuple, OUTPUT_ROLE);
		outEval -> eval ();
		
		// Output element
		outElement.kind      = E_PLUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM

		lastOutputTs = outElement.timestamp;
		
		// Insert the tuple into output synopsis
		if ((rc = outSyn -> insertTuple (outTuple)) != 0)
			return rc;
		
		LOCK_OUT_TUPLE (outTuple);
	}
	
	// Count is 0, remove the count tuple
	else if (count == 0) {
		
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}

int Except::handleLeftMinus (Element inputElement)
{
	int     rc;
	Tuple   inTuple;
	Tuple   countTuple;
	Tuple   outTuple;
	Element outElement;
	int     count;
	
	// Input tuple
	inTuple = inputElement.tuple;	

	// Get the count tuple for this tuple
	if ((rc = getCountTuple_l (inTuple, countTuple)) != 0)
		return rc;
	
	// Decrement the count
	count = --ICOL(countTuple, countCol);
	
	// If the count is nonnegative, we produce a minus element
	// corresponding to this tuple.   
	if (count >= 0) {
		
		// Assert: there exists at least one tuple in the output identical
		// to the input tuple
		if ((rc = getOutTuple (countTuple, outTuple)) != 0)
			return rc;
		
		outElement.kind      = E_MINUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 6 of HR implementation by LAM
		
		lastOutputTs = outElement.timestamp;
		
		// Delete the tuple from the output synopsis
		if ((rc = outSyn -> deleteTuple (outTuple)) != 0)
			return rc;
	}
	
	if (count == 0) {
		
		// Delete the count tuple
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}

// Nearly identical to handleLeftMinus
int Except::handleRightMinus (Element inputElement)
{
	int      rc;
	Tuple    inTuple;
	Tuple    countTuple;
	Tuple    outTuple;
	Element  outElement;
	int      count;
	
	// Input tuple
	inTuple = inputElement.tuple;
	
	// Get the count tuple for this input tuple
	if ((rc = getCountTuple_r (inTuple, countTuple)) != 0)
		return rc;

	// Increment the count
	count = ++ICOL(countTuple, countCol);
	
	// Count is positive: send out an output
	if (count > 0) {
		
		// alloc output tuple
		if ((rc = outStore -> newTuple (outTuple)) != 0)
			return rc;
		
		// copy attributes from count tuple -> outTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		evalContext -> bind (outTuple, OUTPUT_ROLE);		
		outEval -> eval ();
		
		// Output element
		outElement.kind      = E_PLUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 7 of HR implementation by LAM
		
		lastOutputTs = outElement.timestamp;
		
		// Insert the tuple into output synopsis
		if ((rc = outSyn -> insertTuple (outTuple)) != 0)
			return rc;
		
		LOCK_OUT_TUPLE (outTuple);
	}
	
	// Count is 0, remove the count tuple
	else if (count == 0) {
		
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}


int Except::handleRightPlus (Element inputElement)
{
	int     rc;
	Tuple   inTuple;
	Tuple   countTuple;
	Tuple   outTuple;
	Element outElement;
	int     count;
	
	// Input tuple
	inTuple = inputElement.tuple;	
	
	// Get the count tuple for this tuple
	if ((rc = getCountTuple_r (inTuple, countTuple)) != 0)
		return rc;

	// Decrement the count
	count = --ICOL(countTuple, countCol);
	
	// If the count is positive ...
	if (count >= 0) {

		// Assert: there exists at least one tuple in the output identical
		// to the input tuple
		if ((rc = getOutTuple (countTuple, outTuple)) != 0)
			return rc;
		
		outElement.kind      = E_MINUS;
		outElement.tuple     = outTuple;
		outElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (outElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 8 of HR implementation by LAM

		lastOutputTs = outElement.timestamp;
		
		// Delete the tuple from the output synopsis
		if ((rc = outSyn -> deleteTuple (outTuple)) != 0)
			return rc;
	}
	
	if (count == 0) {
		
		// Delete the count tuple
		if ((rc = countSyn -> deleteTuple (countTuple)) != 0)
			return rc;
		
		UNLOCK_COUNT_TUPLE (countTuple);
	}
	
	return 0;
}

int Except::getCountTuple_l (Tuple inTuple, Tuple &countTuple)
{
	int rc;
	TupleIterator *scan;
	
	evalContext -> bind (inTuple, INPUT_ROLE);		
	evalContext -> bind (inTuple, LEFT_ROLE);
	cplsEval -> eval ();
	
	// Get the count tuple for this input tuple if it exists
	if ((rc = countSyn -> getScan (countScanId, scan)) != 0)
		return rc;
	
	// The count tuple does not exist
	if (!scan -> getNext (countTuple)) {
		
		// Create a new count tuple
		if ((rc = countStore -> newTuple (countTuple)) != 0)
			return rc;
		
		// Copy the inTuple's attributes to countTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		initEval -> eval ();
		
		// Initialize count to 0
		ICOL(countTuple, countCol) = 0;
		
		// Insert the count tuple into count synopsis
		if ((rc = countSyn -> insertTuple (countTuple)) != 0)
			return rc;		
	}
	
#ifdef _DM_	
	else {
		// There cannot be more than one tuple that match
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));		
	}
#endif
	
	if ((rc = countSyn -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}

int Except::getCountTuple_r (Tuple inTuple, Tuple &countTuple)
{
	int rc;
	TupleIterator *scan;
	
	evalContext -> bind (inTuple, INPUT_ROLE);
	evalContext -> bind (inTuple, RIGHT_ROLE);
	cprsEval -> eval ();
	
	// Get the count tuple for this input tuple if it exists
	if ((rc = countSyn -> getScan (countScanId, scan)) != 0)
		return rc;
	
	// The count tuple does not exist
	if (!scan -> getNext (countTuple)) {
		
		// Create a new count tuple
		if ((rc = countStore -> newTuple (countTuple)) != 0)
			return rc;

		// Copy the inTuple's attributes to countTuple
		evalContext -> bind (countTuple, COUNT_ROLE);
		initEval -> eval ();
		
		// Initialize count to 0
		ICOL(countTuple, countCol) = 0;
		
		// Insert the count tuple into count synopsis
		if ((rc = countSyn -> insertTuple (countTuple)) != 0)
			return rc;		
	}
	
#ifdef _DM_	
	else {
		// There cannot be more than one tuple that match
		Tuple dummy;
		ASSERT (!scan -> getNext (dummy));		
	}
#endif
	
	if ((rc = countSyn -> releaseScan (countScanId, scan)) != 0)
		return rc;
	
	return 0;
}


int Except::getOutTuple (Tuple countTuple, Tuple &outTuple)
{
	int rc;
	TupleIterator *scan;
	bool bFound;
	
	evalContext -> bind (countTuple, COUNT_ROLE);	
	
	// Get an output tuple identical to the input tuple
	if ((rc = outSyn -> getScan (outScanId, scan)) != 0)
		return rc;
	
	bFound = scan -> getNext (outTuple);
	ASSERT (bFound);
	
	if ((rc = outSyn -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	return 0;
}

//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return  bool true or false
 * true when the statistics have changed and false otherwise
 */ 
bool Except::calculateLocalStats() 
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
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "EXCEPT OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
    //     operator_id, local_cost, num_tuples_processed,local_cost_per_tuple);

  }
  else {
   
    local_selectivity = (double) n_tuples_outputted / (double) num_tuples_processed;
    
    local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed; // /time_unit;
    M_k = local_cost_per_tuple;
    avg_k =  local_cost_per_tuple;
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

void Except::refreshPriority()
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
	 outputs[0]->firstRefresh == true){
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
/** 
 * this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int Except::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return min ( leftInQueue->size(), rightInQueue->size() );
}

// end of part 1 of HR with ready by LAM
int Except::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
