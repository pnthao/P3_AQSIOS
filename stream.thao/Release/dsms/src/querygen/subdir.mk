################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/querygen/log_naive_plan_gen.o \
../dsms/src/querygen/log_plan_gen.o \
../dsms/src/querygen/logexpr.o \
../dsms/src/querygen/logop.o \
../dsms/src/querygen/logop_debug.o \
../dsms/src/querygen/query_debug.o \
../dsms/src/querygen/sem_interp.o 

CC_SRCS += \
../dsms/src/querygen/log_naive_plan_gen.cc \
../dsms/src/querygen/log_plan_gen.cc \
../dsms/src/querygen/logexpr.cc \
../dsms/src/querygen/logop.cc \
../dsms/src/querygen/logop_debug.cc \
../dsms/src/querygen/query_debug.cc \
../dsms/src/querygen/sem_interp.cc 

OBJS += \
./dsms/src/querygen/log_naive_plan_gen.o \
./dsms/src/querygen/log_plan_gen.o \
./dsms/src/querygen/logexpr.o \
./dsms/src/querygen/logop.o \
./dsms/src/querygen/logop_debug.o \
./dsms/src/querygen/query_debug.o \
./dsms/src/querygen/sem_interp.o 

CC_DEPS += \
./dsms/src/querygen/log_naive_plan_gen.d \
./dsms/src/querygen/log_plan_gen.d \
./dsms/src/querygen/logexpr.d \
./dsms/src/querygen/logop.d \
./dsms/src/querygen/logop_debug.d \
./dsms/src/querygen/query_debug.d \
./dsms/src/querygen/sem_interp.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/querygen/%.o: ../dsms/src/querygen/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_CTRL_LOAD_MANAGE_ -D_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/stream.thao/dsms/include" -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


