#ifndef _TIMER_
#define _TIMER_

#ifdef WIN32   // Windows system specific
#include <windows.h>
#else          // Unix based system specific
#include <time.h>
#endif


namespace Monitor {
	class Timer {
	private:

	  /* OLD code
		/// Manually hardcoded clock frequency in Hz (Sponge)
		//static const double CPS = 2800000000.0;
		
		double iCPS;
		
		/// Total time measured by the timer
		unsigned long long int totalTime;
		
		/// The most recent timer start time
		unsigned long long int startTime;
	  */

		// starting time in micro-second
		unsigned long long int startTimeInMicroSec; 
                // ending time in micro-second
		unsigned long long int  endTimeInMicroSec;  
                            
#ifdef WIN32
		LARGE_INTEGER frequency;                    // ticks per second
		LARGE_INTEGER startCount;                   //
		LARGE_INTEGER endCount;                     //
#else
		timespec startCount;                         //
		timespec endCount;                           //
#endif




	public:
		Timer ();
		~Timer ();

		/**
		 * Reset the timer to zero
		 */ 
		int reset ();
		
		/**
		 * Start the timer.
		 */ 
		int start ();
		
		/** 
		 * Stop the timer.
		 */ 
		
		int stop ();
		
		/**
		 * Return the total measured by timer in seconds.
		 */ 
		double getSecs ();

		// Response Time Calculation By Lory Al Moakar
		/** 
		 * return the current time in nsecs
		 */
		unsigned long long int getTime();
		//end of response time calculation by LAM
	};
}

#endif
