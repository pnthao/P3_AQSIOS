#ifndef _TIMER_
#include "execution/monitors/timer.h"
#endif

//#include <time.h>
#include <iostream>

extern unsigned int CPU_SPEED;

using namespace Monitor;
using namespace std;

/** 
 * Modified by Lory Al Moakar
 * the assembly code below is not guaranteed to work on all systems
 * thus we modified it so that we measure the time using software
 * instead of hardware
 */

///OLD CODE //////
/**
 * Assembly code fragment from:
 * 
 * http://www.ncsa.uiuc.edu/UserInfo/Resources/Hardware/IA32LinuxCluster/Doc/timing.html
 * 
 * to get the wall clock time
 

static unsigned long long int nanotime_ia32(void)
{
	unsigned long long int val;
	__asm__ __volatile__("rdtsc" : "=A" (val) : );
	return(val);
}
*/

Timer::Timer ()
{
  /* OLD CODE
     totalTime = 0LL;
	startTime = 0LL;
	iCPS = 1.0 / (1.0 * CPU_SPEED * 1000 * 1000);
  */

#ifdef WIN32
  QueryPerformanceFrequency(&frequency);
  startCount.QuadPart = 0;
  endCount.QuadPart = 0;
#else
  startCount.tv_sec = startCount.tv_nsec = 0;
  endCount.tv_sec = endCount.tv_nsec = 0;
#endif
  
  startTimeInMicroSec = 0;
  endTimeInMicroSec = 0;

}

Timer::~Timer () {}

int Timer::reset ()
{
  /* OLD CODE 
     totalTime = 0LL;
   */
  startTimeInMicroSec = 0;
  endTimeInMicroSec = 0;
  return 0;
}

int Timer::start ()
{
  /* OLD CODE 
	startTime = nanotime_ia32();
  */

#ifdef WIN32
  QueryPerformanceCounter(&startCount);
#else
  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &startCount);
  
#endif
  return 0;

}

int Timer::stop ()
{
  /* OLD CODE
     unsigned long long int endTime;
	
	endTime = nanotime_ia32();
	totalTime += (endTime - startTime);
  */

#ifdef WIN32
    QueryPerformanceCounter(&endCount);
#else
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &endCount);
  
#endif
  
	return 0;
}

double Timer::getSecs ()
{
#ifdef WIN32
  startTimeInMicroSec = startCount.QuadPart * (1000000.0 / frequency.QuadPart);
  endTimeInMicroSec = endCount.QuadPart * (1000000.0 / frequency.QuadPart);
#else
  startTimeInMicroSec = (startCount.tv_sec * 1000000000) + startCount.tv_nsec;
  endTimeInMicroSec = (endCount.tv_sec * 1000000000) + endCount.tv_nsec;
#endif


  return (endTimeInMicroSec - startTimeInMicroSec) * 0.000000001;   
//OLD CODE(totalTime * iCPS);
}

// Response Time Calculation By Lory Al Moakar
unsigned long long int Timer::getTime() { //modified by Thao Pham, now this method read real time

#ifdef WIN32
  QueryPerformanceCounter(&endCount);
  endTimeInMicroSec = endCount.QuadPart * (1000000.0 / frequency.QuadPart);
#else
  clock_gettime(/*CLOCK_PROCESS_CPUTIME_ID, &endCount);*/CLOCK_REALTIME, &endCount);
  endTimeInMicroSec = (endCount.tv_sec * 1E9) + endCount.tv_nsec;
#endif

return  endTimeInMicroSec;
//return getCPUTime();

}
//end of response time calculation by LAM

//realtime tracking (for input rate and possibly response time, by Thao Pham
unsigned long long int Timer::getCPUTime()
{
	#ifdef WIN32
  QueryPerformanceCounter(&endCount);
  endTimeInMicroSec = endCount.QuadPart * (1000000.0 / frequency.QuadPart);
#else
  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &endCount);
  endTimeInMicroSec = (endCount.tv_sec * 1E9) + endCount.tv_nsec;
#endif

  //printf ( "time now is %llu\n", endTimeInMicroSec);
  return  endTimeInMicroSec; 
  
}

//end of realtime tracking by Thao Pham
