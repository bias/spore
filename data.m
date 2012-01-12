%%
load data/25.dat

x=X25(:,1);
y=X25(:,2);
f=X25(:,3);

%%
clear s
g=zeros(74,1);
for i=1:74,
    if x(i) > y(i),
        s(x(i)/2,y(i)/2)=f(i);
        g(i)=f(i);
    end
end


%%
surf(s)
%contour(s)
%mesh(s)
title('N=25 with 3 neighbors and N/2 files')
ylabel('n*2')
xlabel('k''*2')
zlabel('messages')

%%
clear S
for i=1:90,
    S(X(i)/5, Y(i)/5) = F(i);
end

%%
surf(S)
title('3 neighbors, N/2 files and 3*Kp symbols per file.')
ylabel('N*5')
xlabel('Kp*5')
zlabel('messages')