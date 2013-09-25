################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/execution/communicator/communicator.o \
../dsms/src/execution/communicator/node_info.o 

CC_SRCS += \
../dsms/src/execution/communicator/communicator.cc \
../dsms/src/execution/communicator/node_info.cc 

OBJS += \
./dsms/src/execution/communicator/communicator.o \
./dsms/src/execution/communicator/node_info.o 

CC_DEPS += \
./dsms/src/execution/communicator/communicator.d \
./dsms/src/execution/communicator/node_info.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/execution/communicator/%.o: ../dsms/src/execution/communicator/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/p3-aqsios/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


