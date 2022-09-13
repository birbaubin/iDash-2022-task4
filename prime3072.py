import sympy
import secrets

size_q =3072 #choose the security value

p = sympy.nextprime(pow(2, size_q)+secrets.randbits(size_q))
q = (p-1)/2
while sympy.isprime(q)==False:
    p = sympy.nextprime(p)
    q = (p-1)/2
print("p = ", p)
print("q = ", q)