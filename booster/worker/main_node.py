"""
    entry point
"""

from node_proxy_module import node_agent_and_proxy_start


def main():
    booster_node_proxy, booster_agent = node_agent_and_proxy_start()
    booster_node_proxy.proxy_start()
    handle = booster_agent.init_daemons()
    handle.join()


if __name__ == '__main__':
    main()