def test():
    env=ProxyEnv()
    for j in range(100):
        env.reset()
        for i in range(10000):
            state,score,stop,info=env.step(1)
            print(state,score,stop,info)

test()
    