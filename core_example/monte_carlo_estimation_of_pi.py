"""
https://docs.ray.io/en/master/ray-core/examples/monte_carlo_pi.html

2*2 사각형 안의 임의의 점을 이용하는 Monte carlo 방법을 사용해 파이의 값을 계산하는 예제이다.
사각 영역 가운데의 원 안에 점이 포함되는 비율을 이용할 수 있다.
실제 비율은 pi/4라는 것을 알고 있는데 우리가 구한 예측값에 4를 곱하면 대략적인 pi의 값을 얻을 수 잇다

task를 샘플링 작업 분산에 사용하고 actor를 분산된 작업의 진행을 따라가는데 사용할 예정이다. 노트북에서도 실행할 수 있고
정확도를 올리기 위해 좀더 큰 클러스터로 확장도 쉽게 할 수 있다.
"""

import ray
import math
import time
import random

ray.init()

#%% Defining the progress actor
# actor는 상태 저장 가능한 서비스여야 하고, 누구나 인스턴스를 통해 메서드를 호출 할 수 있어야 한다

@ray.remote
class ProgressActor:
    def __init__(self, total_num_samples: int):
        self.total_num_samples = total_num_samples
        self.num_samples_completed_per_task = {}

    def report_progress(self, task_id: int, num_samples_completed: int) -> None:
        # 샘플링 작업할 때 진행상황을 개별적으로 업데이트
        self.num_samples_completed_per_task[task_id] = num_samples_completed

    def get_progress(self) -> float:
        # 전체적인 진행상황 확인 가능
        return (sum(self.num_samples_completed_per_task.values()) / self.total_num_samples)

#%% Defining the Sampling Task
# actor 정의 후, 샘플링 작업을 하고 원 안의 샘플의 개수를 반환하는 task를 정의
# ray task는 상태를 저장하지 않는 함수이고 비동기로 실행되며 병렬로 실행된다.

@ray.remote
def sampling_task(num_samples: int, task_id: int,
                  progress_actor: ray.actor.ActorHandle) -> int:
    num_inside = 0
    for i in range(num_samples):
        x, y = random.uniform(-1, 1), random.uniform(-1, 1)
        if math.hypot(x, y) <= 1:
            num_inside += 1

        # report progress every 1 million samples
        if (i+1) % 1_000_000 == 0:
            # 비동기
            progress_actor.report_progress.remote(task_id, i+1)

    # 마지막 진행
    progress_actor.report_progress.remote(task_id, num_samples)
    return num_inside

#%% Creating a Progress Actor
# actor 정의 후 인스턴스 생성

# 본인의 클러스터 크기에 맞게 변경
NUM_SAMPLING_TASKS = 10
NUM_SAMPLES_PER_TASK = 10_000_000
TOTAL_NUM_SAMPLES = NUM_SAMPLING_TASKS * NUM_SAMPLES_PER_TASK

# progress actor 생성
progress_actor = ProgressActor.remote(TOTAL_NUM_SAMPLES)


#%% Executing SAmpling Tasks
# task가 정의 되었으니 비동기로 실행한다

# 모든 샘플링 태스크는 병렬적으로 생성되고 수행된다
# remote로 task를 실행하는데 이것은 즉시 ObjectRef을 미래로 반환하고 해당 함수는 비동기로 원격 작업 프로세스에서 실행된다
results = [
    sampling_task.remote(NUM_SAMPLES_PER_TASK, i, progress_actor)
    for i in range(NUM_SAMPLING_TASKS)
]


#%% Calling the progress Actor
#  샘플링 task가 진행되는 동안 진행사항을 체크한다

while True:
    progress = ray.get(progress_actor.get_progress.remote())
    print(f"Progress: {int(progress * 100)}%")

    if progress == 1:
        break
    time.sleep(1)


#%% Calculating pi

# 모든 샘플링 task결과 획득
total_num_inside = sum(ray.get(results))
pi = (total_num_inside * 4) / TOTAL_NUM_SAMPLES
print(f"Estimated value of pi is : {pi}")