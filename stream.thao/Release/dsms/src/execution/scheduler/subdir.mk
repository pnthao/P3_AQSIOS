################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/execution/scheduler/lottery_sched_id.o \
../dsms/src/execution/scheduler/lottery_scheduler.o \
../dsms/src/execution/scheduler/round_robin.o 

CC_SRCS += \
../dsms/src/execution/scheduler/lottery_sched_id.cc \
../dsms/src/execution/scheduler/lottery_scheduler.cc \
../dsms/src/execution/scheduler/round_robin.cc 

OBJS += \
./dsms/src/execution/scheduler/lottery_sched_id.o \
./dsms/src/execution/scheduler/lottery_scheduler.o \
./dsms/src/execution/scheduler/round_robin.o 

CC_DEPS += \
./dsms/src/execution/scheduler/lottery_sched_id.d \
./dsms/src/execution/scheduler/lottery_scheduler.d \
./dsms/src/execution/scheduler/round_robin.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/execution/scheduler/%.o: ../dsms/src/execution/scheduler/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_CTRL_LOAD_MANAGE_ -D_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/stream.thao/dsms/include" -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


