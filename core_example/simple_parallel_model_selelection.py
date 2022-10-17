"""
https://docs.ray.io/en/master/ray-core/examples/plot_hyperparameter.html
하이퍼파라미터 튜닝 스크립트를 빨리 작성해서 각 셋을 병렬로 평가하는 예제
두 가지 중요한 Ray API를 사용
- ray.remote : remote 함수 정의
- ray.wait : 결과가 준비될 때까지 대기

"""


#%% Setup : Dependencies
import os
import numpy as np
from filelock import FileLock

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

import ray
ray.shutdown()
ray.init()

# 시도 해 볼 랜덤 하이퍼파라미터 세트 개수
num_evaluations = 10

# 랜덤 하이퍼 파라미터 생성 함수 정의
def generate_hyperparameters():
    return {
        "learning_rate": 10 ** np.random.uniform(-5, 1),
        "batch_size": np.random.randint(1, 100),
        "momentum": np.random.uniform(0, 1)
    }

def get_data_loaders(batch_size):
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307, ), (0.3081,))]
    )
    # 여러 작업들이 데이터 다운로드를 시도 할 것임 -> 오버라이트 유발
    # FileLock 사용
    with FileLock(os.path.expanduser("~/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data", train=True, download=True, transform=mnist_transforms
            ),
            batch_size=batch_size,
            shuffle=True,
        )
    test_loader = torch.utils.data.DataLoader(datasets.MNIST("~/data", train=False,
                                                             transform=mnist_transforms),
                                              batch_size=batch_size,
                                              shuffle=True)
    return train_loader, test_loader

#%% Defining the Neural Network

class ConvNet(nn.Module):
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)


def train(model, optimizer, train_loader, device=torch.device("cpu")):
    """
    학습의 간소화를 위해 1024 샘플로 자름
    """
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if batch_idx * len(data) > 1024:
            return
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()

def test(model, test_loader, device=torch.device("cpu")):
    """
    모델의 정확도 확인. 간소화를 위해 512 샘플
    """
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            if batch_idx * len(data) > 512:
                break
            data, target = data.to(device), target.to(device)
            output = model(data)
            _, predicted = torch.max(output.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct/total


#%% Evaluating the Hyperparameters
# 위에서 생성된 네트워크는 학습되고 정확도를 반환한다.
# 이 학습된 네ㅡ워크는 베스트셋의 하이퍼파라미터를 찾을 것이다

@ray.remote
def evaluate_hypterparameters(config):
    model = ConvNet()
    train_loader, test_loader = get_data_loaders(config["batch_size"])
    optimizer = optim.SGD(
        model.parameters(), lr=config["learning_rate"], momentum=config["momentum"]
    )
    train(model, optimizer, train_loader)
    return test(model, test_loader)

#%%Syschronous Evaluation of Randomly Generated Hyperparameters
# 병렬적으로 평가되는 하이퍼파라미터 세트 생성

best_hyperparameters = None
best_accuracy = 0
remaining_ids = []
hyperparameters_mapping = {}

# accuracy_id : ObjectRef(remote task를 컨트롤)
for i in range(num_evaluations):
    hyperparameters = generate_hyperparameters()
    accuracy_id = evaluate_hypterparameters.remote(hyperparameters)
    remaining_ids.append(accuracy_id)
    hyperparameters_mapping[accuracy_id] = hyperparameters


while remaining_ids:
    # ray.wait를 사용해 완성된 첫 번째 task object ref 를 얻을 수 있음
    done_ids, remaining_ids = ray.wait(remaining_ids)
    result_id = done_ids[0]

    hyperparameters = hyperparameters_mapping[result_id]
    accuracy = ray.get(result_id)
    print(
        f"""We achieve accuracy {100*accuracy}% with
        learning_rate : {hyperparameters["learning_rate"]},
        batch_size: {hyperparameters["batch_size"]}
        momentum: {hyperparameters["momentum"]}
        """
    )
    if accuracy > best_accuracy:
        best_hyperparameters = hyperparameters
        best_accuracy = accuracy

# 최고 성능의 하이퍼파라미터 기록
print(
        f"""Best accuracy over {num_evaluations} trials was {100*best_accuracy} with
        learning_rate : {best_hyperparameters["learning_rate"]},
        batch_size: {best_hyperparameters["batch_size"]}
        momentum: {best_hyperparameters["momentum"]}
        """
    )
