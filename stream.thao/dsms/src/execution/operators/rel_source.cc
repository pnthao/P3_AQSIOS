/**
 * @file     rel_source.cc
 * @date     Sep 8, 2004
 * @brief    Relation source implementation
 */

#ifndef _REL_SOURCE_
#include "execution/operators/rel_source.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#include <string.h>
#include <math.h>

using namespace Execution;
using namespace std;

#define LOCK_OUTPUT_TUPLE(t) (store -> addRef ((t)))

RelSource::RelSource (unsigned int id, ostream &_LOG)
	: LOG (_LOG) 
{
	this -> id = id;
	this -> outputQueue = 0;
	this -> source = 0;
	this -> store = 0;
	this -> rel = 0;
	this -> evalContext = 0;
	this -> lastOutputTs = 0;
	this -> lastInputTs = 0;
	this -> numAttrs = 0;
	
	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = REL_SOURCE;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	// we want input operators to have a high priority 
	// because we want to input tuples at a fast pace
	// as close as possible to when they arrive
	this -> priority = 10; 
	this -> num_tuples_processed = 0;

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

RelSource::~RelSource ()
{
	if (evalContext)
		delete evalContext;
	
}

//----------------------------------------------------------------------
// Initialization routines
//----------------------------------------------------------------------

int RelSource::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int RelSource::setSource (Interface::TableSource *source)
{
	ASSERT (source);

	this -> source = source;
	return 0;
}

int RelSource::setStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> store = store;
	return 0;
}

int RelSource::setSynopsis (RelationSynopsis *syn)
{
	ASSERT (syn);
	
	this -> rel = syn;
	return 0;
}

int RelSource::setScan (unsigned int scanId)
{
	this -> scanId = scanId;
	return 0;
}

int RelSource::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int RelSource::addAttr (Type type, unsigned int len, Column outCol)
{
	ASSERT (numAttrs < MAX_ATTRS);
	
	attrs [numAttrs].type = type;
	attrs [numAttrs].len = len;
	
	offsets [numAttrs] = (numAttrs > 0)?
		(offsets [numAttrs - 1] + attrs [numAttrs - 1].len) : DATA_OFFSET;		
	
	outCols [numAttrs] = outCol;
	
	numAttrs++;
	return 0;
}

int RelSource::setMinusTuple (Tuple tuple)
{
	this -> minusTuple = tuple;
	return 0;
}

int RelSource::initialize ()
{
	int rc;
	
	if ((rc = source -> start()) != 0)
		return rc;
	return 0;
}

int RelSource::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	char *inputTuple;
	unsigned int inputTupleLen;
	Timestamp inputTupleTs;
	char inputTupleSign;
	Tuple outTuple;
	Element outElement;
	bool bHeartbeat;

#ifdef _MONITOR_
	startTimer ();
#endif							

	numElements = timeSlice;
	
	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham
	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We are stalled at the output queue.
		if (outputQueue -> isFull())
			break;
		
		// Get the next input tuple
		if ((rc = source -> getNext (inputTuple,
					     inputTupleLen,
					     bHeartbeat)) != 0)
		  return rc;
		 	
		// We do not have an input tuple (element) to process
		if (!inputTuple)
			break;
		
		//local cost computation, by Thao Pham (selectivity of output is always 1)
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 2 of local cost computation, by Thao Pham
		
		// Timestamp 
		memcpy(&inputTupleTs, inputTuple, TIMESTAMP_SIZE);

		// We should have a progress of time.
		if (lastInputTs > inputTupleTs) {
			LOG << "RelationSource: input not in timestamp order" << endl;
			return -1;
		}		
		lastInputTs = inputTupleTs;		
		
		// ignore heartbeats
		if (bHeartbeat) {
			LOG << "Relationsource: Heartbeat received" << endl;
			continue;
		}
		LOG << "Relationsource: Tuple received" << endl;
		
		// All data tuple lengths are fixed 
		ASSERT (inputTupleLen ==
				offsets[numAttrs-1] + attrs[numAttrs-1].len);		
		
		// Sign
		inputTupleSign = inputTuple [SIGN_OFFSET];
		
		if ((inputTupleSign != PLUS) && (inputTupleSign != MINUS)) {
			LOG << "RelationSource: Invalid sign for tuple" << endl;
			return -1;
		}
		
		if (inputTupleSign == PLUS) { 
			if ((rc = store -> newTuple (outTuple)) != 0) 
				return rc;
			
			if ((rc = decodeAttrs (inputTuple, outTuple)) != 0) 
				return rc;
			
			if ((rc = rel -> insertTuple (outTuple)) != 0) 
				return rc;
			LOCK_OUTPUT_TUPLE (outTuple);
		}
		
		else {			
			
			if ((rc = decodeAttrs (inputTuple, minusTuple)) != 0) 
				return rc;
			
			if ((rc = getSynTuple (minusTuple, outTuple)) != 0) 
				return rc;

			if ((rc = rel -> deleteTuple (outTuple)) != 0)
				return rc;
			
		}
		
		outElement.kind = (inputTupleSign == '+')? E_PLUS : E_MINUS;
		outElement.tuple = outTuple;
		outElement.timestamp = inputTupleTs;
		
		outputQueue -> enqueue (outElement);
		

		lastOutputTs = inputTupleTs;

		/*//Lottery scheduling by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		
		// find the number of tuples inputted so far
		n_tuples_inputted += 1;
		
		// end of part 1 of Lottery scheduling by LAM*/
	}

	// Heartbeat generation: Assert to the operator above that we won't
	// produce any element with timestamp < lastInputTs
	
	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
	}

	
#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);		
#endif
	
	//local cost computation, by Thao Pham
	//get current time	
	unsigned long long int timeAfterLoop = mytime.getCPUTime();
	
	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1  
	  local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 3 of cost computation by Thao Pham
	
       
	return 0;
}

int RelSource::decodeAttrs (char *inTuple, Tuple outTuple)
{	      
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		switch (attrs [a].type) {
		case INT:
			memcpy (&ICOL(outTuple, outCols[a]),
					inTuple + offsets[a], INT_SIZE);				
			break;
				
		case FLOAT:
			memcpy (&FCOL(outTuple, outCols[a]),
					inTuple + offsets[a], FLOAT_SIZE);						
			break;
				
		case CHAR:
			strncpy (CCOL(outTuple, outCols[a]),
					 inTuple + offsets[a], attrs [a].len);
			break;
			
		case BYTE:
			BCOL (outTuple, outCols[a]) = inTuple [offsets[a]];
			break;
			
		default:
			ASSERT (0);
			break;
		}
	}
	
	return 0;
}

int RelSource::getSynTuple (Tuple inTuple, Tuple &outTuple)
{
	int rc;
	TupleIterator *scan;
	
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	if ((rc = rel -> getScan (scanId, scan)) != 0)
		return rc;
	
	if (!scan -> getNext (outTuple)) {
		if ((rc = rel -> releaseScan (scanId, scan)) != 0)
			return rc;
		return -1;
	}
	
	if ((rc = rel -> releaseScan (scanId, scan)) != 0)
		return rc;	
	
	return 0;
}

//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return  void
 */ 
bool RelSource::calculateLocalStats(){
	
//implementation added by Thao Pham

// no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0) {
    return false;
  }
  //calculate local cost
  //selectivity is always 1, as initialized 
  if (!firstRefresh) { 
  	
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
      //      printf("SELECT OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "SELECTION OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
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
//end of the implementation added by Thao Pham
	
	
//load manager, by Thao Pham
  snapshot_local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed;
  //end of load manager, by Thao Pham 
 return true; 
 }

/** this method is used in order to calculate the priority
 * of the operator. This is used in order to assign the priorities
 * used by the scheduler to schedule operators.
 */

void  RelSource::refreshPriority(){}
// end of part 1 of HR implementation by LAM

//HR with ready by Lory Al Moakar 
/** this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int RelSource::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return 1;
}

// end of part 1 of HR with ready by LAM
int RelSource::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
