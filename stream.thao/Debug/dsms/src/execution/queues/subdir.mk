################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/execution/queues/shared_queue_reader.o \
../dsms/src/execution/queues/shared_queue_writer.o \
../dsms/src/execution/queues/simple_queue.o 

CC_SRCS += \
../dsms/src/execution/queues/shared_queue_reader.cc \
../dsms/src/execution/queues/shared_queue_writer.cc \
../dsms/src/execution/queues/simple_queue.cc 

OBJS += \
./dsms/src/execution/queues/shared_queue_reader.o \
./dsms/src/execution/queues/shared_queue_writer.o \
./dsms/src/execution/queues/simple_queue.o 

CC_DEPS += \
./dsms/src/execution/queues/shared_queue_reader.d \
./dsms/src/execution/queues/shared_queue_writer.d \
./dsms/src/execution/queues/simple_queue.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/execution/queues/%.o: ../dsms/src/execution/queues/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/PITT/workspace/p3/P3_AQSIOS/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


