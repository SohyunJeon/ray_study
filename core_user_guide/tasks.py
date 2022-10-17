
#%% Tasks

import ray
import time
ray.shutdown()
ray.init()

#%%
def normal_function():
    return 1

# @ray.remote 데코레이터를 붙임으로써 ray remote function으로 정의할 수 있음
@ray.remote
def my_function():
    return 1

# 위의 remote function을 적용하기 위해 'remote' method 사용
# 이는 즉각 object ref(미래시점에)을 반환하고 task를 생성한다.
obj_ref = my_function.remote()

# 결과는 ray.get으로 얻을 수 있다
assert ray.get(obj_ref) == 1

@ray.remote
def slow_function():
    time.sleep(10)
    return 1

# 병렬로 실행되는 ray remote function
# ray의 내부 이벤트 반복으로 실행
for _ in range(4):
    slow_function.remote()


#%% Passing object refs to remote function
# object refs는 remote functions으로 통과할 수 있다.
# 함수가 실제 실행될 때 인자는 파이썬 기본 값으로 전달됨

@ray.remote
def function_with_an_argument(value):
    return value+1

obj_ref = my_function.remote()
assert ray.get(obj_ref) == 1

# object ref를 인자로써 다른 ray remote function에 넘겨줄 수 있다
obj_ref2 = function_with_an_argument.remote(obj_ref)
assert ray.get(obj_ref2) == 2

# 두번째 작업은 첫번째 작업이 종료되기 전까지 실행되지 않음. 왜냐면 첫번째 작업에 종속되어 있기 때문
# 두 작업이 다른 머신에 할당되어 있다면 첫번째 작업의 결과는 network를 통해 두번째 작업이 예정되어 있는 머신으로 감



#%% Waiting for partial results
# 작업들이 시작되면 그 중에서 막힘없이 어떤 작업이 끝날지 ray.get을 통해 알고싶을 것이다.
# 그 때 작업은 ray.wait를 이용해  끝날 수 있다
ready_refs, remainig_refs = ray.wait(obj_refs, num_returns=1, timeout=None)


#%% Multiple returns

@ray.remote(num_returns=3)
def return_multiple():
    return 0, 1, 2

a, b, c = return_multiple.remote()

# multiple object 반환 작업에서 ray는 메모리 사용 감소를 위해 제너레이터를 지원한다.
@ray.remote(num_returns=3)
def return_multiple_as_generator():
    for i in range(3):
        yield i

# 리턴값이 전부 생성되기 전까지는 해당 객체들은 사용할 수 없다
a, b, c = return_multiple_as_generator.remote()



#%% Cancelling tasks
# ray.cancel를 이용해 object ref를 반환하면서 함수 취소

@ray.remote
def blocking_operation():
    time.sleep(10e6)

obj_ref = blocking_operation.remote()
ray.cancel(obj_ref)

from ray.exceptions import TaskCancelledError
try:
    ray.get(obj_ref)
except TaskCancelledError:
    print("Object reference was cancelled.")


#%%Specifying Required Resources
# 리소스 자원을 특정하고 싶을 때 ray는 자동으로 가능한 CPU, GPU를 추출한다.
# 자원을 특정하지 않으면 기본값은 cpu 1개이다.
# GPU를 특정하는 경우 CUDA_VISIBLE_DEVICES 환경설정같이 가시적 형태의 격리를 제공하지만 결국 작업에 책임이 있는 것은 실제 GPU를 사용하는 작업이다.
# (예를들어 Tensorflow, Pytorch같은 딥러닝 프레임 워크)


# 필요한 자원을 특정
@ray.remote(num_cpus=4, num_gpus=8)
def my_function():
    return 1

# ray는 또한 부분적인 리소스 요청도 지원한다
@ray.remote(num_gpus=0.5)
def h():
    return 1

# 또한 ray는 고객 리소스도 지원한다
@ray.remote(resources={'Custom':1})
def f():
    return 1




