 /**
 * @file       QC_scheduler.cc
 * @date       May 5, 2009
 * @brief      Implementation of query class scheduling using several
 * HR schedulers
 * @author     Lory Al Moakar
 */
#include <iostream>
//to generate random numbers
#include <stdlib.h> 
#include <math.h>
#include <stdio.h>
#include <thread_db.h>
//#include <algorithm>

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _QC_SCHEDULER_
#include "execution/scheduler/QC_scheduler.h"
#endif

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef COMMUNICATOR_H_
#include"execution/communicator/communicator.h"
#endif

#ifndef NODE_INFO_H_
#include "execution/communicator/node_info.h"
#endif

using namespace std;
using namespace Execution;

static const TimeSlice timeSlice = 100;//100000;//100;//100000;

// the default value for cycle length in tuples.
static const int CYCLE_LENGTH_D = 200;

//the starting priority value
static const double STARTING_PRIORITY = 10.0;



QCScheduler::QCScheduler (){
  
  //load manager, by Thao Pham
  this->num_loadMgrs = 0;
  //loadMgrs = new LoadManager* [MAX_QUERY_CLASSES];
    //end of load manager, by Thao Pham
  //initialize the number of query classes.
  n_query_classes = 1;

  bStop = false;
  // Response Time Calculation By Lory Al Moakar
  //initialize time_unit to 1 if not specified by user
  // or not using server impl to start the system
  time_unit = 1;
  //end of part 1 of response time calculation by LAM
  //if user did not specify the number of times 
  //to refresh the priorities before we reach 
  //steady state, we assume it is 0 i.e. 
  // the system starts in steady state
  STEADY_STATE = 0;  

  // initialize quota_default to 100
  quota_default = 100;
  
  //add a scheduler for each query class
  for ( int i = 0; i < n_query_classes; i++ ) {
    QCHRScheduler * sc = new QCHRScheduler();
    scheds.push_back( sc );
    quota.push_back( quota_default );
  }
  //unless specified we honor the user priorities
  honor_priorities = true;
  
  //load management, by Thao Pham
  count_loadManagementCycle = 0;
  //end of load management, by Thao Pham
}
QCScheduler::QCScheduler ( int n_classes){
  
  //load manager, by Thao Pham
  //loadMgrs = new LoadManager* [MAX_QUERY_CLASSES];
  this->num_loadMgrs = 0;
  //end of load manager, by Thao Pham
  
  //initialize the number of query classes.
  n_query_classes = n_classes;

  bStop = false;
  // Response Time Calculation By Lory Al Moakar
  //initialize time_unit to 1 if not specified by user
  // or not using server impl to start the system
  time_unit = 1;
  //end of part 1 of response time calculation by LAM
  //if user did not specify the number of times 
  //to refresh the priorities before we reach 
  //steady state, we assume it is 0 i.e. 
  // the system starts in steady state
  STEADY_STATE = 0;  

  // initialize quota_default to 100
  quota_default = 3;
  
  //add a scheduler for each query class
  for ( int i = 0; i < n_query_classes; i++ ) {
    QCHRScheduler * sc = new QCHRScheduler();
    scheds.push_back( sc );
    quota.push_back( quota_default );
  }
  //fprintf(stdout,"\nscheduler initialized\n"); 
  //unless specified we honor the user priorities
  honor_priorities = true;
  
  //load management, by Thao Pham
  count_loadManagementCycle = 0;
  //end of load management, by Thao Pham
  
}		
QCScheduler::~QCScheduler () {  }
		
		
int QCScheduler::addOperator (Operator *op) { 
  fprintf(stdout,"\n %d id is %d \n", op->operator_id, op->query_class_id);

  assert (op->query_class_id >= 0 && op->query_class_id < n_query_classes);
  // add the operator to the scheduler that is 
  // responsible for its query class.
  //fprintf(stdout,"\nOP added\n");
  return  scheds[ op->query_class_id ] -> addOperator(op);
 

}

	       	
int QCScheduler::run (long long int numTimeUnits) { 

  //initialize a communication object  to communicate to the global coordinator
  Node_Info *node_info = new Node_Info(this->n_query_classes);
  Communicator* comm = new Communicator("paros.cs.pitt.edu",8000,node_info);
  //set the initial list of active queries


  comm->start();


  //fprintf(stdout,"\nSCHEDULER STARTED \n");
  //declare a response variable to return in case of errors
  int rc = 0;
  //if we are rounding up the priorities
  if ( !honor_priorities ) {
    printf("Not honoring priorities :P \n");
    //only keep the relative priority by dividing each priority 
    // by the minimum priority
    // note that this will not be preserved that much 
    //since we are doing ceil a few lines below when we calculate the
    //.quota of each scheduler
    for ( int i = 0; i < n_query_classes; i++ ) 
      query_class_priorities[i] /= query_class_priorities[n_query_classes-1];

  }
  
  //synergy with load_managing, by Thao Pham
  //keep an original copy of the priorities
  for(int i=0;i<n_query_classes;i++)
  	original_query_class_priorities[i] = query_class_priorities[i];
  	
  //end of part 1 of synergy with load managing by Thao Pham
  
  //calculate total_priorities
  double total_priorities = 0;
  for ( int i = 0; i < n_query_classes; i++ ) 
    total_priorities +=  query_class_priorities[i];
  if ( !(total_priorities > 0) ) 
    fprintf( stderr, "total priorities is %f", total_priorities ) ;

  // Response Time Calculation By Lory Al Moakar
  // get the time when the system starts execution
  Monitor::Timer mytime;
  this -> startingTime = mytime.getTime();
  //added by Thao Pham, to keep track of real start time
  this->startingCPUTime = mytime.getCPUTime(); 
  
 //a variable used to store current time when needed
  long long int curTime = 0;
  
  //initialize the state of the schedulers here
  for ( int i = 0;  i <  n_query_classes; i++ ) {
    scheds[i]->initialize( startingTime, time_unit, STEADY_STATE );
    quota[i] = ceil(quota_default * query_class_priorities[i] / total_priorities );
    printf ( "class %d gets %d units \n", i, quota[i] );
    if ( STEADY_STATE == 0 ) 
      scheds[i]-> setSteadyState();
  }
  
   
    
    
  // numtimeunits == 0 signal for scheduler to run forever (until stopped)
  if (numTimeUnits == 0) {		
    while (!bStop) {
      
      //start from the beginning of the list
      for ( int i = 0; i < n_query_classes; i++ ) {
	if ( quota[i] <= 0 ) {
	  quota[i] += ceil( quota_default * query_class_priorities[i] / total_priorities );
	  
	}
	else {
	  rc = scheds[i] -> run(quota[i]);
	  quota[i] = ceil( quota_default * query_class_priorities[i] / total_priorities );
	  if (rc <= 0) 
	    return rc;
	  else if ( rc > 0 ) { 
	    quota[i] -= rc;
	  }
//load manager, by Thao Pham


 #ifdef _LOAD_MANAGE_

				Monitor::Timer ctrlTimer;
					
				long long unsigned int ctrl_now = ctrlTimer.getTime();
				if(ctrl_last_ts ==0)
					ctrl_last_ts = ctrl_now;
				
				//if(numofCycles==loadManagingCycle)
				if(ctrl_now - ctrl_last_ts >= 0.9*CONTROL_PERIOD )
				{
					count_loadManagementCycle ++;
					for(int k = 0; k<num_loadMgrs; k++)
					{
						//printf("load manager: %d\n", loadMgr->load_manager_type);
						switch(loadMgrs[k]->load_manager_type)
						{
							case 0: break;						
							case 1: //ALM
									if((loadMgrs[k]->run(true, 6))<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							/*case 2: //CTRL
									if((loadMgrs[k]->runCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 3: //HCTRL
									if((loadMgrs[k]->runDynamicCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 4: //extCTRL
									if((loadMgrs[k]->runExtendedCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 5: //extHCTRL
									if((loadMgrs[k]->runExtendedDynamicCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
							case 6: //extBaseline
									if((loadMgrs[k]->runExtendedBaselineCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
							case 7: //baseline1 -- with long-term avg cost
									if((loadMgrs[k]->runBaselineLoadMgr1())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 8: //baseline1 -- with long-term avg cost
									if((loadMgrs[k]->runBaselineLoadMgr2())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;*/
						}
					}	
			
			
					if(count_loadManagementCycle == SCHED_ADJUSTMENT_CYCLE)
					{
						ResourceBalancing_G();
						count_loadManagementCycle = 0;
					}
				}		
				
#endif //def _LOAD_MANAGE
	}
	
      }
      if ( STEADY_STATE != 0 ) {
	steadySystem();
      }
    }
    curTime = mytime.getCPUTime();
    curTime -= this->startingCPUTime;
    curTime = (long long int)((double)curTime/(double)time_unit);
    
      
    //printf("ran for %llu timeunits started at %llu", curTime, startingTime);
    
  }
  
  
  else {		
    // Response Time Calculation By Lory Al Moakar
    //get the current time
    //int initial_wait = 2;
    //fprintf(stderr,"HERERERE");
    do {
      
      //start from the beginning of the list
      for ( int i = 0; i < n_query_classes; i++ ) {
	if ( quota[i] <= 0 ) {
	  quota[i] += ceil( quota_default * query_class_priorities[i] / total_priorities );
	 // fprintf( stdout, "not enough quota %d \n ", i);
	}
	else {
	  //fprintf(stdout, "running %d \n ", i ) ;
	  rc = scheds[i] -> run(quota[i]);
	  quota[i] = ceil(quota_default  * query_class_priorities[i] / total_priorities );
	  if (rc < 0) 
	    return rc;
	  else if ( rc > 0 ) 
	    quota[i] -= rc;
	    
	    //load manager, by Thao Pham


 #ifdef _LOAD_MANAGE_

				Monitor::Timer ctrlTimer;
					
				long long unsigned int ctrl_now = ctrlTimer.getTime();
				if(ctrl_last_ts ==0)
					ctrl_last_ts = ctrl_now;
				
				//if(numofCycles==loadManagingCycle)
				if(ctrl_now - ctrl_last_ts >= 0.9*CONTROL_PERIOD )
				{
					count_loadManagementCycle ++;
					for(int k = 0; k<num_loadMgrs; k++)
					{
						//printf("load manager: %d\n", loadMgr->load_manager_type);
						switch(loadMgrs[k]->load_manager_type)
						{
							case 0: break;						
							case 1: //ALM
									if((loadMgrs[k]->run(true, 6))<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							/*case 2: //CTRL
									if((loadMgrs[k]->runCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 3: //HCTRL
									if((loadMgrs[k]->runDynamicCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 4: //extCTRL
									if((loadMgrs[k]->runExtendedCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 5: //extHCTRL
									if((loadMgrs[k]->runExtendedDynamicCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
							case 6: //extBaseline
									if((loadMgrs[k]->runExtendedBaselineCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
							case 7: //baseline1 -- with long-term avg cost
									if((loadMgrs[k]->runBaselineLoadMgr1())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
									
							case 8: //baseline1 -- with long-term avg cost
									if((loadMgrs[k]->runBaselineLoadMgr2())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;*/
						}
					}
					if(count_loadManagementCycle == SCHED_ADJUSTMENT_CYCLE)
					{
						ResourceBalancing_G();
						updateNodeInfo(comm);
						for(int i=0;i<num_loadMgrs; i++)
							loadMgrs[i]->resetCapacityUsageTracking();
						 
						  	//for(int i=0;i<num_loadMgrs;i++)
						  	//	printf("\n%0.1f :", query_class_priorities[i] );
						count_loadManagementCycle = 0;
					}
				}
				
#endif //def _LOAD_MANAGE 
	}
	if ( STEADY_STATE != 0 ) {
	  steadySystem();
	}
	curTime = mytime.getCPUTime();
	curTime -= this->startingCPUTime;
	curTime = (long long int)((double)curTime/(double)time_unit);
	if ( curTime < 0 )  { 
	  printf("ERROR GOING BACK IN TIME");
	  return -1656;
	}
	
	/*if ( curTime >= numTimeUnits){
	  printf( "Time ended %lld unit is %d \n",  
		  numTimeUnits, time_unit);
	  
	}*/
	if(bStop) printf("Mystereous stop");
      }
    } while (!bStop && curTime < numTimeUnits); 
    //end of part 3 of response time calculation by LAM
  }
  
  
  printf("ran for %llu timeunits started at %llu", curTime, startingTime);
  printf(" Now is : %llu \n", mytime.getTime());

  //close communication with the coordinator
  comm->closeCommunication();
  printf("communication closed\n");

  delete comm;

  return 0; 
  
}
int QCScheduler::stop () { 
  bStop = true;
  return 0; 

}
int QCScheduler::resume () { 
  bStop = false;
  return 0; 
}

void QCScheduler::updateNodeInfo(Communicator * comm){
	pthread_mutex_lock(&comm->mutexUpdateReady);
	comm->isUpdateReady = 1;

	//update the node info
	Node_Info* ni = comm->nodeInfo;

	//update classes' capacity
	for(int i=0;i<n_query_classes;i++){
		ni->set_capacity(i,this->loadMgrs[i]->headroom_factor);
		ni->set_capacity_usage(i,this->loadMgrs[i]->avg_capacity_usage);

		ni->set_local_priority(i,this->getOriginalPriority(i)); //this is the normalized local "current" priority that the node must honor when the class needs
	}

	pthread_cond_signal(&comm->condUpdateReady);
	pthread_mutex_unlock(&comm->mutexUpdateReady);
}



/**
 * this method goes over the operators
 * and finds out if the system is in steady 
 * state or not
 */
bool QCScheduler::steadySystem () {
  bool steady = true;
  
  for (unsigned int o = 1 ; o < n_query_classes ; o++) {
    if ( !scheds[o]->steadySystem() ) 
      steady = false;
  }
  if ( !steady ) return true;
  // the system is operating in steady state
  setSteadyState();
  //set the steady_state variable to 0 so that this method
  //does not get invoked again.
  STEADY_STATE = 0;
  return true;
}
 

/** 
 * this method is used in order to set the state of the system to steady
 * It is used by the QC_scheduler to set the state 
 * of the individual schedulers to steady.
 */
void QCScheduler::setSteadyState(){
  
  printf("SYSTEM OPERATING IN STEADY STATE ");
  for (unsigned int o = 0 ; o < n_query_classes; o++) {
    scheds[o]->setSteadyState();
  }
}

