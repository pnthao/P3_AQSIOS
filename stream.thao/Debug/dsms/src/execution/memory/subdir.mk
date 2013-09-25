################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/execution/memory/memory_mgr.o 

CC_SRCS += \
../dsms/src/execution/memory/memory_mgr.cc 

OBJS += \
./dsms/src/execution/memory/memory_mgr.o 

CC_DEPS += \
./dsms/src/execution/memory/memory_mgr.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/execution/memory/%.o: ../dsms/src/execution/memory/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/p3-aqsios/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


