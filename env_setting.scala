cygwin

After the first time installation, need to install two extra 
ssh
inetutil

Once done, cygwin is ready for use
set up passwordless
ssh-keygen
ssh-copy-id paslechoix@gw01.itversity.com

Once done, use
ssh paslechoix@gw01.itversity.com
no password will be asked for.

Once in cygwin, you can see it provides interconnection between local host and the cygwin (linux environment)
cd c

transfer file:
scp /cygdrive/c/<local win7 host path> paslechoix@gw01.itversity.com:/home/paslechoix<path on remote>
