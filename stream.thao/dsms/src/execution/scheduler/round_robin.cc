/**
 * @file       round_robin.cc
 * @date       June 4, 2004
 * @brief      Implementation of the round robin scheduler
 */

/// debug
#include <iostream>
using namespace std;

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _ROUND_ROBIN_
#include "execution/scheduler/round_robin.h"
#endif

using namespace Execution;

static const TimeSlice timeSlice = 10;//50;//100;
//load managing, by Thao Pham
static const int loadManagingCycle = 10; 
//end of load managing, by Thao Pham 

RoundRobinScheduler::RoundRobinScheduler()
{
	this -> numOps = 0;	
	bStop = false;
	// Response Time Calculation By Lory Al Moakar
	//initialize time_unit to 1 if not specified by user
	// or not using server impl to start the system
	time_unit = 1;
	//end of part 1 of response time calculation by LAM
	
	//load managing, by Thao Pham
	ctrl_last_ts = 0;
	//end of load managing, by Thao Pham
}

RoundRobinScheduler::~RoundRobinScheduler() {}

int RoundRobinScheduler::addOperator (Operator *op)
{
	if (numOps == MAX_OPS)
		return -1;	
	ops [numOps ++] = op;
	
	return 0;
}

int RoundRobinScheduler::run (long long int numTimeUnits)
{
	//sleep a bit - 1s, waiting for the system to be stable	
	/*Monitor::Timer sleepTimer;
	unsigned long long int bTime = sleepTimer.getTime(); 
	while (sleepTimer.getTime()-bTime < 120000000000);
	*/
	sleep(10);
	int rc;
	int numofCycles = 0;
	// Response Time Calculation By Lory Al Moakar
	// get the time when the system starts execution
	Monitor::Timer mytime;
	this -> startingTime = mytime.getTime(); 
	//added by Thao Pham, to keep track of real start time
	this->startingCPUTime = mytime.getCPUTime();
	
	//initialize the system_start_time in each of the operators
	for (unsigned int o = 0 ; o < numOps ; o++) {
	  ops [o] -> system_start_time = this -> startingTime;
	  //added by Thao Pham, to keep track of real time
	  ops[o] -> system_start_CPUtime = this->startingCPUTime;
	  ops [o] -> time_unit = this -> time_unit;
	}

	//end of part 2 of response time calculation by LAM

	// numtimeunits == 0 signal for scheduler to run forever (until stopped)
	if (numTimeUnits == 0) {		
		while (!bStop) {
			
			for (unsigned int o = 0 ; o < numOps ; o++) {
				if ((rc = ops [o] -> run(timeSlice)) != 0) {
					return rc;
				}

#ifdef _LOAD_MANAGE_

				Monitor::Timer ctrlTimer;
					
				long long unsigned int ctrl_now = ctrlTimer.getTime();
				if(ctrl_last_ts ==0)
					ctrl_last_ts = ctrl_now;
				
				//if(numofCycles==loadManagingCycle)
				if(ctrl_now - ctrl_last_ts >= 0.9*CONTROL_PERIOD )
				{
					//printf("load manager: %d\n", loadMgr->load_manager_type);
					switch(loadMgrs[0]->load_manager_type)
					{
						case 0: break;						
						case 1: //ALM
								if((loadMgrs[0]->run(true))<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
								
						/*case 2: //CTRL
								if((loadMgrs[0]->runCtrlLoadMgr())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
								
						case 3: //HCTRL
								if((loadMgrs[0]->runDynamicCtrlLoadMgr())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
								
						case 4: //extCTRL
								if((loadMgrs[0]->runExtendedCtrlLoadMgr())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
								
						case 5: //extHCTRL
								if((loadMgrs[0]->runExtendedDynamicCtrlLoadMgr())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
						case 6: //extBaseline
								if((loadMgrs[0]->runExtendedBaselineCtrlLoadMgr())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
						case 7: //baseline1 -- with long-term avg cost
								if((loadMgrs[0]->runBaselineLoadMgr1())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
								
						case 8: //baseline1 -- with long-term avg cost
								if((loadMgrs[0]->runBaselineLoadMgr2())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break; */
					}
					
				}
				
#endif //def _LOAD_MANAGE
			    //numofCycles++;
			  	
				//end of load managing, by Thao Pham
			}
					
				    //numofCycles++;
				  	
					//end of load managing, by Thao Pham
		
		}
		
	}
	
	else {		
	  // Response Time Calculation By Lory Al Moakar
	  //for (long long int t = 0 ; (t < numTimeUnits) && !bStop ; t++) {
	  //get the current time
	  Monitor::Timer mytime;
	  unsigned long long int curTime = mytime.getCPUTime();
	  curTime -= this->startingCPUTime;
	  
	  do {
			
		    for (unsigned int o = 0 ; o < numOps ; o++) {
		     	 if ((rc = ops [o] -> run(timeSlice)) != 0) {
					return rc;
		     	 }
		     	 //printf("%d\n",o); 

#ifdef _LOAD_MANAGE_		      
			    Monitor::Timer ctrlTimer;
					
					long long unsigned int ctrl_now = ctrlTimer.getTime();
					if(ctrl_last_ts ==0)
						ctrl_last_ts = ctrl_now;
					
					//if(numofCycles==loadManagingCycle)

					if(ctrl_now - ctrl_last_ts >= 0.9*CONTROL_PERIOD )
					{
						//printf("%lld \n", ctrl_now - ctrl_last_ts);
						//printf("load manager: %d\n", loadMgr->load_manager_type);
						switch(loadMgrs[0]->load_manager_type)
						{
							
							case 0: //no shedding
									break;							
							case 1: //ALM
									if((loadMgrs[0]->run(true))<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
								
							/*case 2: //CTRL
									if((loadMgrs[0]->runCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
								
							case 3: //HCTRL
									if((loadMgrs[0]->runDynamicCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
								
							case 4: //extCTRL
									if((loadMgrs[0]->runExtendedCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
								
							case 5: //extHCTRL
									if((loadMgrs[0]->runExtendedDynamicCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
							case 6: //extBaseline
									if((loadMgrs[0]->runExtendedBaselineCtrlLoadMgr())<0)
									return rc; 
									//numofCycles = 0;
									ctrl_last_ts = ctrl_now;
									break;
							case 7: //baseline1 -- with long-term avg cost
								if((loadMgrs[0]->runBaselineLoadMgr1())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break;
								
							case 8: //baseline1 -- with long-term avg cost
								if((loadMgrs[0]->runBaselineLoadMgr2())<0)
								return rc; 
								//numofCycles = 0;
								ctrl_last_ts = ctrl_now;
								break; */
							
						}
					}
					
#endif //_LOAD_MANAGE_
				    //numofCycles++;
				  	
					//end of load managing, by Thao Pham
		    }
						
		    curTime = mytime.getCPUTime();
		    curTime -= this->startingCPUTime;
		    curTime = curTime/time_unit;
		    
		    
		     
	  } while (!bStop && curTime < numTimeUnits); 
	  //end of part 3 of response time calculation by LAM
	}
#ifdef _CTRL_LOAD_MANAGE_
loadMgrs[0]->incoming_tuples_monitor();
#endif	
	
	return 0;
}



int RoundRobinScheduler::stop ()
{
	bStop = true;
	return 0;
}

int RoundRobinScheduler::resume ()
{
	bStop = false;
	return 0;
}
