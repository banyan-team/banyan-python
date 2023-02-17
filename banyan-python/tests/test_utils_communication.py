import hashlib
import random
import string
import boto3
import multiprocessing
import pytest

from banyan.utils_communication import configure_worker, get_invocation_idx, get_num_invocations, get_num_processes, get_num_workers, get_pipe, get_process_idx, get_queue_url, get_worker_config, get_worker_idx, is_main_worker, is_same_invocation

sqs = boto3.resource("sqs")


def random_string(n):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(n))


def launch_func(
    num_workers,
    invocation_idx,
    scatter_queue_url,
    gather_queue_url,
    shuffle_queue_urls,
    process_idx,
    process_pipes,
    func
):
    configure_worker(
        num_workers,
        invocation_idx,
        scatter_queue_url,
        gather_queue_url,
        shuffle_queue_urls,
        process_idx,
        process_pipes,
    )
    func(
        num_workers,
        invocation_idx,
        scatter_queue_url,
        gather_queue_url,
        shuffle_queue_urls,
        process_idx,
        process_pipes,
    )

num_setups = 0

@pytest.fixture(scope="session", params=[1, 7])  # [1, 2, 6, 7, 12, 13, 15]
def setup_workers(request):
    global num_setups

    num_workers = request.param
    session_id = random_string(8)

    scatter_queue = sqs.create_queue(
        QueueName=f"bn_test_scatter_queue_{session_id}_{num_setups}", Attributes={"ReceiveMessageWaitTimeSeconds": "20"}
    )
    gather_queue = sqs.create_queue(
        QueueName=f"bn_test_gather_queue_{session_id}_{num_setups}", Attributes={"ReceiveMessageWaitTimeSeconds": "20"}
    )

    shuffle_queue_urls = []
    process_pipes = []
    for i in range(num_workers // 6):
        # Create shuffle queue
        queue_name = f"bn_test_shuffle_queue_{session_id}_{num_setups}_{i}"
        queue = sqs.create_queue(
            QueueName=queue_name, Attributes={"ReceiveMessageWaitTimeSeconds": "20"}
        )
        shuffle_queue_urls.append(queue.url)

        # Create Pipe
        process_pipes.append([multiprocessing.Pipe(duplex=True) for _ in range(6)])

    def run_all_processes(func):
        # Start Process for each worker
        processes = []
        process_pipes = None
        all_pipes = []
        for i in range(num_workers):
            # Create pipes
            if i % 6 == 0:
                process_pipes = [multiprocessing.Pipe(duplex=True) for _ in range(6)]
                all_pipes.extend(process_pipes)

            # Configure process to launch
            launch_func_args = (
                num_workers,
                i // 6,
                scatter_queue.url,
                gather_queue.url,
                shuffle_queue_urls,
                i % 6,
                (
                    [conn1 for conn1, _ in process_pipes]
                    if (i % 6 == 0)
                    else process_pipes[i % 6][1]
                ),
                func
            )

            # Launch it
            p = multiprocessing.Process(target=launch_func, args=launch_func_args)
            p.start()
            processes.append(p)

        # Join Processes to complete test computation
        for process in processes:
            process.join()
        for conn1, conn2 in all_pipes:
            conn1.close()
            conn2.close()

    # Finalize by destroying all SQS queues
    def finalize():
        for queue_url in shuffle_queue_urls:
            queue = sqs.Queue(queue_url)
            queue.delete()
        scatter_queue.delete()
        gather_queue.delete()

    request.addfinalizer(finalize)

    # Update num_setups
    num_setups += 1

    return run_all_processes

def setup_workers_func(num_processes, process_idx, scatter_queue_url, gather_queue_url, shuffle_queue_urls, invocation_idx, process_pipes):
    assert get_num_workers() > 0 and get_num_workers() - 1 >= get_worker_idx()
    assert get_num_invocations() > 0 and get_num_invocations() - 1 >= get_invocation_idx()
    assert get_invocation_idx() == invocation_idx
    assert is_same_invocation(get_worker_idx(), (get_worker_idx() + 1) % 6)
    assert get_process_idx() == process_idx
    assert get_num_processes() > 0 and get_num_processes() - 1 >= get_process_idx()
    assert get_num_processes() == num_processes
    assert get_invocation_idx
    assert is_main_worker() == (get_worker_idx() == 0)

    if get_worker_idx() == 0:
        assert get_process_idx() == 0
    if get_process_idx() == 0:
        assert len(set([get_pipe(i) for i in range(6)])) == 6
    else:
        assert get_pipe() == get_pipe(0)

    wc = get_worker_config()
    assert len(wc["shuffle_queue_urls"]) > 0
    assert get_queue_url(src="client") == wc["scatter_queue_url"]
    assert get_queue_url(dst="client") == wc["gather_queue_url"]
    assert get_queue_url(src="client") != get_queue_url(dst="client")

def test_setup_workers(setup_workers):
    setup_workers(setup_workers_func)