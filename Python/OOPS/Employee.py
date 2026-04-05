class Employee:
    def __init__(self):
        self.__salary = 0 # private attribute
        self.employeeName="" # public attribute

    def setName(self,name):
        self.employeeName=name
    
    def setSalary(self,val):
        self.__salary=val
    
    def getSalary(self):
        return self.__salary

obj1 = Employee()
obj1.setName("Raj")
obj1.setSalary(10000)
obj2 = Employee()
obj2.setName("Rahul")
obj2.setSalary(15000)

# print("Salary of " + obj1.employeeName + " is " + str(obj1.getSalary()))
# print("Salary of " + obj2.employeeName + " is " + str(obj2.getSalary()))


class Student:
    def __init__(self):
        self.name=""
        self.rollNumber=0
    
    def setDetails(self,name,rollNumber):
        self.name=name
        self.rollNumber=rollNumber
    
    def displayDetails(self):
        print("Name :",self.name)
        print("Roll Number :" ,str(self.rollNumber))

student1 = Student()
student1.setDetails("Alice", 101)
student1.displayDetails()
