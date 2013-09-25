/**
 * @file       QCHR_scheduler.cc
 * @date       May 4, 2009
 * @brief      Implementation of HR scheduler as described in paper used by QC_scheduler
 * @author     Lory Al Moakar
 */
#include <iostream>
//to generate random numbers
#include <cstdlib> 

using namespace std;

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _QC_HR_SCHEDULER_
#include "execution/scheduler/QCHR_scheduler.h"
#endif

// we need to distinguish output operators 
#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif


#include <math.h>
#include <algorithm>
#include <stdio.h>


using namespace Execution;

static const TimeSlice timeSlice = 50;//100000;//100;//100000;

// the default value for cycle length in tuples.
static const int CYCLE_LENGTH_D = 200;

//the starting priority value
static const double STARTING_PRIORITY = 10.0;


QCHRScheduler::QCHRScheduler (){

	this -> numOps = 0;
	this -> numSrOps = 0;
	bStop = false;
	// Response Time Calculation By Lory Al Moakar
	//initialize time_unit to 1 if not specified by user
	// or not using server impl to start the system
	time_unit = 1;
	//end of part 1 of response time calculation by LAM

	//set the default cycle length
	cycle_length = CYCLE_LENGTH_D ;
	tuples_in_cycle = 0;
	n_refreshes = 0;
	n_ready = 0;
	n_not_ready = 0;

}		
QCHRScheduler::~QCHRScheduler () {  }


int QCHRScheduler::addOperator (Operator *op) { 
	if ( (numOps+numSrOps) == MAX_OPS)
		return -1654;
	//set n_tuples_inputted to 0 so that all operators start up
	//with zero
	op -> n_tuples_inputted = 0;
	op -> stdev = 0;
	op -> avg_k = 0;
	//set the visited variable to false
	op -> visited = false;
	//if the operator is  a source operator --> add to sr_ops
	if ( op->operator_type == STREAM_SOURCE
			|| op->operator_type == REL_SOURCE
			|| op->operator_type == SYS_STREAM_GEN
			|| op->isShedder==true) {
		sr_ops.push_back(op);
		++numSrOps;
	}
	else {
		//this is a non-source operator -->
		//add the operator to the ops list
		ops.push_back(op);
		++numOps;
	}
	return 0;

}


int QCHRScheduler::run (long long int numTimeUnits) { 

	assert (numTimeUnits>0);
	//fprintf(stdout, "\nSCHEDULER STARTED \n");
	//declare a response variable to return in case of errors
	int rc = 0;

	if (  this -> numOps == 0 )
		return 0;
	// Response Time Calculation By Lory Al Moakar

	Monitor::Timer mytime;
	this -> startingTime = mytime.getTime();
	//added by Thao Pham, to keep track of real start time
	this->startingCPUTime = mytime.getCPUTime();

	//a variable used to store current time when needed
	long long int curTime = 0;

	int n_in_sr_mode = 0;

	// numtimeunits == 0 signal for scheduler to run forever (until stopped)
	if (numTimeUnits == 0) {
		while (!bStop) {

			//start from the beginning of the list
			//since it is sorted, we are starting from the operator with the highest
			//priority
			int op_index = 0;
			int source_mode = 0;
			//while the current op is not ready
			while ( ops[op_index]->b_active==false || ops[op_index] ->readyToExecute() <= 0 ) {
				++op_index;
				++ n_not_ready;
				//none of the operators is ready to execute

				if ( op_index == numOps ) {
					source_mode = 1;
					break;
					//return -1712;
				}
			}

			//printf( "Running Operator %d \t", ops[op_index]->operator_id);
			if ( !source_mode ) {
				++n_ready;
				if ((rc = ops [op_index] -> run(timeSlice)) != 0) {
					return rc;
				}
			}
			//if we need more tuples
			else {
				++n_in_sr_mode;
				if ( n_in_sr_mode == 2 ) return 0;
				for ( int i = 0; i < numSrOps; i++ ) {
					if(sr_ops[i]->b_active){
						if(sr_ops[i]->isShedder==true){
							if ((rc = sr_ops[i]->run_with_shedder(timeSlice)) != 0)
								return rc;
						}
						else{
							if ((rc = sr_ops[i] -> run(timeSlice)) != 0)
								return rc;
						}
						tuples_in_cycle += sr_ops[i]->n_tuples_inputted;
						sr_ops[i]->n_tuples_inputted = 0;
					}
				}
			}
			curTime = mytime.getCPUTime();
			curTime -= this->startingCPUTime;
			curTime = (long long int)((double)curTime/(double)time_unit);


			//printf("ran for %llu timeunits started at %llu", curTime, startingTime);



			//calculate how many tuples have been inputted so far in this cycle
			//tuples_in_cycle += ops[op_index]->n_tuples_inputted;
			//ops[op_index]->n_tuples_inputted = 0;
			//Is this cycle over ?
			if ( tuples_in_cycle >= cycle_length ) {
				//reset the number of tuples
				tuples_in_cycle = 0;

				// refresh the priorities of the operators
				// and this method also resorts the ops vector
				// by those new priorities
				refreshPriorities();

				//increment n_refreshes so that we can keep track
				// of the number of times the statistics were refreshed.
				++n_refreshes;
				//printf("Printing priorities:\n");
				//for (unsigned int o = 0 ; o < numOps ; o++) {
				//  printf ( "op: %d \t n_tickets: %d \n", o, n_tickets[o]);
				//}
			}
		}
	}

	else {
		// Response Time Calculation By Lory Al Moakar
		//for (long long int t = 0 ; (t < numTimeUnits) && !bStop ; t++) {
		//get the current time
		Monitor::Timer mytime;
		curTime = mytime.getCPUTime();
		//curTime = this->startingTime;
		//fprintf(stderr,"HERERERE");
		do {
			//start from the beginning of the list
			//since it is sorted, we are starting from the operator with the highest
			//priority
			int op_index = 0;
			int source_mode = 0;
			//fprintf(stderr,"before loop %d", numOps);
			//while the current op is not ready
			while (ops[op_index]->b_active==false || ops[op_index] ->readyToExecute() <= 0 ) {
				++op_index;
				++ n_not_ready;
				//fprintf(stderr,"op index is %d\t",op_index);
				// none of the operators is ready to execute
				//--> switch to source mode
				if ( op_index == numOps ) {
					source_mode = 1;
					//fprintf(stderr, "SOURCE MODE %d", op_index);
					break;
					//return -1712;
				}
			}
			if ( !source_mode ) {
				++n_ready;
				if ((rc = ops [op_index] -> run(timeSlice)) != 0) {
					return rc;
				}
			}
			//if we need more tuples
			else {
				//printf("SOURCE MODE");
				++n_in_sr_mode;
				if ( n_in_sr_mode == 2 ) return 0;
				for ( int i = 0; i < numSrOps; i++ ) {
					if(sr_ops[i]->b_active){

						if(sr_ops[i]->isShedder==true){
							if ((rc = sr_ops[i]->run_with_shedder(timeSlice)) != 0)
								return rc;
						}
						else{
							if ((rc = sr_ops[i] -> run(timeSlice)) != 0)
								return rc;
						}
						//calculate how many tuples have been processed inputted so far in this cycle
						tuples_in_cycle += sr_ops[i]->n_tuples_inputted;
						sr_ops[i]->n_tuples_inputted = 0;
					}
				}
				//Is this cycle over ?
				if ( tuples_in_cycle >= cycle_length ) {
					//reset the number of tuples
					tuples_in_cycle = 0;

					// refresh the priorities of the operators
					// and this method also resorts the ops vector
					// by those new priorities
					refreshPriorities();

					//increment n_refreshes so that we can keep track
					// of the number of times the statistics were refreshed.
					++n_refreshes;

					//printf("Printing priorities:\n");
					//for (unsigned int o = 0 ; o < numOps ; o++) {
					// printf ( "op: %d \t n_tickets: %d priority %f Type %d \n", o, n_tickets[o]
					//   , ops[o]->priority, ops[o]->operator_type);
					//}


				}

			}

			curTime = mytime.getCPUTime();
			curTime -= this->startingCPUTime;
			curTime = (long long int)((double)curTime/(double)time_unit);
			if ( curTime < 0 )  {
				printf("ERROR GOING BACK IN TIME");
				return -1656;
			}
			/*
      if ( curTime >= numTimeUnits){
	printf( "Time ended %lld unit is %d \n",  
		numTimeUnits, time_unit);

      }*/
			if(bStop) printf("Mystereous stop");

		} while (!bStop && curTime < numTimeUnits);
		//end of part 3 of response time calculation by LAM
	}

	//printf("n-refreshes = %d\n", n_refreshes);
	//printf("ran for %llu timeunits started at %llu", curTime, startingTime);
	//printf(" Now is : %llu \n", mytime.getTime());
	//printf( "ready ops is %d Not ready ops is %d \n", n_ready, n_not_ready );
	refreshPriorities();
	//printf("Printing priorities:\n");
	//for (unsigned int o = 0 ; o < numOps ; o++) {
	//  printf ( "op: %d \t  priority %f Type %d selectivity %f cost %f\n",
	//     ops[o]->operator_id, ops[o]->priority, ops[o]->operator_type,
	//     ops[o]-> local_selectivity, ops[o]->local_cost_per_tuple);
	//}
	return (curTime - numTimeUnits);

}
int QCHRScheduler::stop () { 
	bStop = true;
	return 0;

}
int QCHRScheduler::resume () { 
	bStop = false;
	return 0;
}

//this method is used to compare the operators in order to determine their
//order for the scheduler
bool compare_ops_priority ( Operator * op1 , Operator * op2 ) {
	return ( op1->priority > op2 -> priority );
}


//HR implementation by Lory Al Moakar

int QCHRScheduler::refreshPriorities() {

	std::vector <Operator*> opList ;
	for (unsigned int o = 0 ; o < numOps ; o++) {
		ops[o]-> n_refreshes += ops[o]->calculateLocalStats();
		//printf("op %d tuples %d output %d selectivity %f cost %f\n",
		//   o,
		//   ops[o]->num_tuples_processed,
		//   ops[o]->n_tuples_outputted,
		//   ops[o]->local_selectivity,
		//   ops[o]->local_cost_per_tuple);
		ops[o]->visited = false;
		if ( ops[o]->operator_type == OUTPUT ||
				ops[o]->operator_type == SINK ){
			for ( int i = 0; i < ops[o]->numInputs; i++ )
				opList.push_back(ops[o]->inputs[i]);
			ops[o]->visited = true;

			//ops[o]->refreshPriority();

		} //end of if output operator
	} // endof for loop

	while( !opList.empty()) {
		//printf("oplist is not empty %d\n", opList.size());
		//printf("head op is %d size is %d\n", opList[0]-> operator_type, opList.size());
		//for ( int x = 0; x < opList.size(); x++ )
		// printf ( "opType %d \t", opList[x]->operator_type);

		if ( opList[0]->visited == true) {
			opList.erase(opList.begin());
		}
		else if ( opList[0]->operator_type == REL_SOURCE ||
				opList[0]->operator_type == STREAM_SOURCE ||
				opList[0]->operator_type == SYS_STREAM_GEN  ) {
			opList[0]->visited = true;
			opList.erase(opList.begin());

		}//end of if operator is an input op
		//is this an operator that is not shared
		else  if ( opList[0]->numOutputs == 1 ) {
			opList[0]->refreshPriority();
			for ( int i = 0; i < opList[0]->numInputs; i++ )
				opList.insert(opList.begin()+1,opList[0]->inputs[i]);
			opList[0]->visited = true;
			opList.erase(opList.begin());



		}//end if operator is not shared

		else {

			bool outputs_visited = true;
			bool same_scheduler = true;
			//have its inputs been visited
			for ( int i = 0; i< opList[0]->numOutputs; i++ ){
				if ( opList[0]->outputs[i]->visited == false )
					outputs_visited = false;
				if (opList[0]->outputs[i]->query_class_id != opList[0]->query_class_id)
					same_scheduler = false;
			}// end of for loop

			if (outputs_visited ) {
				if ( opList[0]->num_tuples_processed != 0)
					opList[0]->refreshPriority();
				for ( int i = 0; i < opList[0]->numInputs; i++ )
					opList.insert(opList.begin()+1,opList[0]->inputs[i]);
				opList[0]->visited = true;
				opList.erase(opList.begin());

			}
			else if ( same_scheduler ) {
				opList.push_back(opList[0]);
				opList.erase(opList.begin());
			}
			//outputs not visited and they are not on the same scheduler
			else {
				//create a list to put the children in
				//so that we set all their visited to true
				std::vector <Operator*> children;
				children.push_back(opList[0]);

				while ( !children.empty()){
					//set the operator's visited to true
					children[0]->visited = true;
					//add its inputs to the children list
					for ( int c = 0; c < children[0]->numInputs; c++ )
						children.push_back(children[0]->inputs[c]);
					//remove this parent from list
					children.erase(children.begin());
				}
				opList.erase( opList.begin() );

			}//end of else --> outputs not visited & not same scheduler
		} // end of operator is shared
	}//end of while  opList !empty

	//call the method to assign dynamic priorities to
	//source and output operators
	assignPriToSrcOut();



	//sort the operators in the ops list in decreasing order of priorities
	stable_sort( ops.begin(), ops.end(), compare_ops_priority) ;

	return 0;
}

/** 
 * this method is used to assign priorities to source
 * and output operators.
 * @return int 0 success !0 failure
 */
int QCHRScheduler::assignPriToSrcOut() {

	//assign this range_win_priority to join operators
	for ( unsigned int o = 0; o < numOps; o++ ) {
		if ( (ops[o]->operator_type == BIN_JOIN ||
				ops[o]->operator_type == BIN_STR_JOIN)
				&& ops[o]->n_refreshes != 0 ){
			//assigning the priority of a join operator to
			//be the max priority among its input operator
			ops[o]->priority = ops[o]->inputs[0]->priority;
			if ( ops[o]->inputs[0]->priority < ops[o]->inputs[1]->priority ) {
				ops[o]->priority = ops[o]->inputs[1]->priority;
				ops[o]->inputs[0]->priority = ops[o]->inputs[1]->priority;
			}
			else
				ops[o]->inputs[1]->priority = ops[o]->inputs[0]->priority;

			//ops[o] -> priority = ops[o]->priority * 2;
			//  printf ( "PR op %d %f \n", o,  ops[o]->priority);
		}
	}

	//assign this join priority to istream  operators
	for ( unsigned int o = 0; o < numOps; o++ ) {
		if ( ops[o]->operator_type == ISTREAM &&
				ops[o]->n_refreshes != 0 ){
			//assigning the priority of an istream  operator to
			//be the priority of its input operator
			ops[o]->priority = ops[o]->inputs[0]->priority;
			//  printf ( "PR op %d %f \n", o,  ops[o]->priority);
		}
	}




	//assign this max_priority to output operators
	for ( unsigned int o = 0; o < numOps; o++ ) {
		if ( ops[o]->operator_type == OUTPUT ||
				ops[o]->operator_type == SINK  ){
			//assigning the priority of an output operator to
			//be the priority of its input operator
			ops[o]->priority = ops[o]->inputs[0]->priority;
			//printf ( "PR op %d %f \n", o,  ops[o]->priority);
		}

	}//end of third for loop


	return 0;

}


// end of part 1 of HR implementation by LAM


/**
 * this method goes over the operators
 * and finds out if the system is in steady 
 * state or not
 */
bool QCHRScheduler::steadySystem () {

	for (unsigned int o = 1 ; o < numOps ; o++) {
		if ( ops[o]-> n_refreshes < STEADY_STATE && !(
				ops[o]->operator_type == OUTPUT ||
				//ops[o]->operator_type == REL_SOURCE ||
				ops[o]->operator_type == SINK //||
				//ops[o]->operator_type == STREAM_SOURCE ||
				//ops[o]->operator_type == SYS_STREAM_GEN )){
		)){
			//printf ( "not ready op is %d n_refreshes %d\n ",
			//    ops[o]-> operator_id, ops[o]-> n_refreshes);
			return false;
		}
	}
	return true;
}



//Query Class Scheduling by Lory Al Moakar
/** 
 * this method is used by the QC_scheduler in order to initialize
 * the state of this scheduler.
 * @return int 0 success !0 failure
 */

int QCHRScheduler::initialize(long long int starting_t, int time_u, int st_state){
	//initialize the priorities of the operators to starting_priority
	for (unsigned int o = 0 ; o < numOps ; o++) {
		//We start scheduling in a round robin fashion
		ops[o] -> priority = STARTING_PRIORITY;
		ops[o] -> n_refreshes = 0;

	}
	//initialize the state of this scheduler
	//-> its starting_time, time_unit and steady_state  variable.
	this -> startingTime = starting_t;
	this -> time_unit = time_u;
	this -> STEADY_STATE = st_state;

	//feed starting time as a seed to the random number generator
	srand(this -> startingTime);

	//fprintf(stderr,"before loop 3");
	//initialize the system_start_time in each of the operators
	for (unsigned int o = 0 ; o < numOps ; o++) {
		ops [o] -> system_start_time = this -> startingTime;
		//added by Thao Pham, to keep track of real time
		ops[o] -> system_start_CPUtime = this->startingCPUTime;
		ops [o] -> time_unit = this -> time_unit;

	}
	//initialize the start time, time unit, priority and number of refreshes
	//in each of the source operators.
	for ( unsigned int i = 0; i < numSrOps; i++ )  {
		sr_ops [i] -> system_start_time = this -> startingTime;
		sr_ops [i] -> time_unit = this -> time_unit;
		sr_ops[i] -> priority = STARTING_PRIORITY;
		sr_ops[i] -> n_refreshes = 0;
	}
	/* if the user wishes to calculate stats even before system reaches steady state
  if ( STEADY_STATE == 0 ) {
    for (unsigned int o = 0 ; o < numOps ; o++) {
      if ( ops[o]->operator_type == OUTPUT) {
	((Execution::Output * )(ops[o]))-> steady_state = true;
      }
    }
  }
	 */

}

/** 
 * this method is used in order to set the state of the system to steady
 * It is used by the QC_scheduler to set the state 
 * of the individual schedulers to steady.
 */
void QCHRScheduler::setSteadyState(){

	//printf("SYSTEM OPERATING IN STEADY STATE ");
	for (unsigned int o = 0 ; o < numOps ; o++) {
		//set the steady_state variable in the output ops
		// to true so that we start calculating response time
		if ( ops[o]->operator_type == OUTPUT) {
			((Execution::Output * )(ops[o]))-> steady_state = true;
		}
	}
}


//end of Query Class Scheduling by LAM
