"""
task와 actor는 object를 생성하고 계산하는데 이 object를 remote objects로 언급한다.
왜냐면 이것들은 ray 클러스터 어디에나 저장될 수 있기 때문이다.
우리는 object refs를 사용해 remote objects를 참조한다.

object ref는 필수적으로 포인터나 id를 가지고 있다. 이 id는 remote object의 값을 보지 않더라도 참조할 때 사용된다.
object refs는 아래 두가지 방법으로 생성될 수 있다.
1. remote function 호출
2. put(docstring)을 사용한 반환

"""

import ray
import time
ray.shutdown()
ray.init()


#%% Fetch Object Data
# get 함수를 사용해서 object ref로부터의 remote object의 결과를 가져올 수 있다.
# 만약 현재 노드의 object 저장소가 해당 Object를 가지고 있지 않다면 다운로드 한다.

obj_ref = ray.put(1)
assert ray.get(obj_ref) == 1

# get을 이용해 timeout을 세팅할 수 있다.
from ray.exceptions import GetTimeoutError

@ray.remote
def long_running_function():
    time.sleep(8)

obj_ref = long_running_function.remote()
try:
    ray.get(obj_ref, timeout=4)
except GetTimeoutError:
    print("'get' timed out.")


#%% Passing Object Arguments

# ray 객체 참조는 ray 어플리케이션 주위에 자유롭게 전달될 수 있다. 즉, task, actor 함수에 대한 인수로 전달되고 다른 개체에 저장할 수도 있다.
# 객체는 분산 참조 카운팅을 통해 추적되며 개체에 대한 모든 참조가 삭제되면 해당 데이터가 자동으로 해제된다.

# 개체를 ray task 또는 actor method에 전달할 수 있는 방법은 두 가지가 있다.
# 개체가 전달되는 방식에 따라 ray 는 작업 실행 전에 개체를 역참조할지 여부를 결정한다

# 1. Passing an object as top-level argument : 객체를 최상위 인수로 전달
# 최상위 인수로 직접 전달, ray는 객체를 역참조 => ray가 모든 최상위 개체 참조 인수에 대한 기본 데이터를 가져오고
# 개체 데이터를 완전히 사용할 수 있을 때까지 작업을 실행하지 않음을 의미

@ray.remote
def echo(a: int, b: int, c: int):
    print(a, b, c)

# 값(1,2,3)을 task에 바로 전달
echo.remote(1,2,3)

# 값(1,2,3)을 ray object 저장소에 입력
a, b, c = ray.put(1), ray.put(2), ray.put(3)

# object를 top-level 인자로 echo에 전달. ray는 인자를 역참조. -> echo는 문자 그대로의 값(1,2,3)을 볼 수 있다
echo.remote(a,b,c)



# 2. Passing an object as a nested argument : 객체를 중첩 인수로 전달
# 객체가 중첨 객체 내에서 전달되면 (e.g.,python list 내에서) ray는 이를 역참조 하지 않는다.
# => 이것은 task가 구체적인 값을 가져오기 위해 참조에서 ray.get()을 호출해야 함을 의미한다.
# 하지만 task가 ray.get()을 호출하지 않으면 개체 값을 task가 시스템으로 전송할 필요하 없다.
# 가능한 경우 객체를 최상위 인수로 전달하는 것이 좋지만 중첩된 인수는 데이터를 볼 필요 없이 다른 작업에 객체를 전달하는 데 유용할 수 있다.

@ray.remote
def echo_and_get(x_list):
    print('args: ', x_list)
    print('values: ', ray.get(x_list))

a, b, c = ray.put(1), ray.put(2), ray.put(3)
echo_and_get.remote([a, b, c])


#%% Closure Capture of Objects
# 또한 closure-capture를 통해 task로 객체를 전달 할 수 있다. 이는 많은 task나 actor사이에서 큰 객체를 공유하고 싶으면서 해당 인자를
# 반복해서는 넘기고 싶지 않을때 편리한 방법이다.
# 하지만 객체 참조를 종료하는 task를 정의하는 것은 참조 카운팅을 통해 객체를 고정하므로 객체는 직압이 완료될 때까지 제거되지 않는다.

a, b, c = ray.put(1), ray.put(2), ray.put(3)

@ray.remote
def print_via_capture():
    print(ray.get([a, b, c]))

print_via_capture.remote()