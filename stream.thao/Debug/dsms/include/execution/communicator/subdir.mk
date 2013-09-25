################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CC_SRCS += \
../dsms/include/execution/communicator/communicator.cc 

OBJS += \
./dsms/include/execution/communicator/communicator.o 

CC_DEPS += \
./dsms/include/execution/communicator/communicator.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/include/execution/communicator/%.o: ../dsms/include/execution/communicator/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/p3-aqsios/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


