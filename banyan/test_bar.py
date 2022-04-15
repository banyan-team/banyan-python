import progressbar
from tqdm import tqdm

import time
from time import sleep


# widgets = ['Loading: ', progressbar.AnimatedMarker()]
# bar = progressbar.ProgressBar(widgets=widgets).start()

# for i in range(50):
#     time.sleep(0.1)
#     bar.update(i)
#     # bar.next()
pbar = tqdm(desc = "creating cluster")
i = 0
while i < 100:
    pbar.update(i)
    # print('hii' + str(i))
    sleep(0.1)
    i += 1
# valuee = tqdm(range(100))
# for i in valuee:
#     sleep(0.02)