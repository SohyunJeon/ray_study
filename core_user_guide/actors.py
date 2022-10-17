"""
Actors는 RAY API에서 함수가 클래스로 확장된 개념이다.
새로운 actor가 생성되면 새 worker가 생성되고, actor의 method는 특정 worker에 스케쥴링되어 접근 및 사용될 수 있다

"""


import ray
import time
ray.shutdown()
ray.init()

#%%

@ray.remote
class Counter():
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def get_counter(self):
        return self.value

counter = Counter.remote()

#%% Specifying required resources

@ray.remote(num_cpus=2, num_gpus=0.5)
class Actor(object):
    pass

#%% Calling the actor
#remote 를 통해 method를 호출할 수 있다.
# get을 통해 실제 값을 받을 수 있다.
obj_ref = counter.increment.remote()
assert ray.get(obj_ref) == 1


# 다른 actor의 method는 병렬적으로 실행되고, 동일 actor의 method는 순차적으로 실행된다.
# 동일 actor의 method는 상태를 공유한다
counter = [Counter.remote() for _ in range(10)]
# 아래 작업은 병행적으로 실행됨
results = ray.get([c.increment.remote() for c in counter])
print(results)
# 첫 번째 인스턴스의 method를 다섯번 실행 -> 순차적으로 실행
results = ray.get([counter[0].increment.remote() for _ in range(5)])
print(results)


#%% Passing Around Actor Handles
# actor핸들은 다른 task로 전달될 수 있다. actor 핸들을 사용하는 remote 함수를 정의할 수 있다

import time
@ray.remote
def f(counter):
    for _ in range(1000):
        time.sleep(0.1)
        counter.increment.remote()

counter = Counter.remote()
[f.remote(counter) for _ in range(3)]

for _ in range(10):
    time.sleep(1)
    print(ray.get(counter.get_counter.remote()))