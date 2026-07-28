from producer.user_simulator import UserSimulator

simulator = UserSimulator()

for _ in range(20):
    user = simulator.get_user()
    print(user)
    

