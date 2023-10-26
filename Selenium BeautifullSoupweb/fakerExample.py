from faker import Faker

fake_data=Faker()

print(fake_data.name() )

for _ in range(10):
    print(fake_data.name())