begin(T1)
begin(T2)
fail(3); fail(4)
R(T1,x1)
W(T2,x8,88)
end(T1)
recover(4); recover(3)
R(T2,x3)
end(T2)