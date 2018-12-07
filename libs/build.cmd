@echo off
if not exist target mkdir target
if not exist target\classes mkdir target\classes


echo compile classes
javac -nowarn -d target\classes -sourcepath jvm -cp "d:\cxcache\jni4net.corext.0.8.6.1\lib\jni4net.j-0.8.6.0.jar"; "jvm\dstsauthentication\Claim.java" "jvm\dstsauthentication\AuthenticationResult.java" "jvm\dstsauthentication\DstsAuthentication.java" 
IF %ERRORLEVEL% NEQ 0 goto end


echo DstsAuthentication.j4n.jar 
jar cvf DstsAuthentication.j4n.jar  -C target\classes "dstsauthentication\Claim.class"  -C target\classes "dstsauthentication\AuthenticationResult.class"  -C target\classes "dstsauthentication\DstsAuthentication.class"  > nul 
IF %ERRORLEVEL% NEQ 0 goto end


echo DstsAuthentication.j4n.dll 
csc /nologo /warn:0 /t:library /out:DstsAuthentication.j4n.dll /recurse:clr\*.cs  /reference:"D:\CxCache\Jni4net.Corext.0.8.6.1\samples\AuthenticationDemo\work\DstsAuthentication.dll" /reference:"D:\CxCache\Jni4net.Corext.0.8.6.1\lib\jni4net.n-0.8.6.0.dll"
IF %ERRORLEVEL% NEQ 0 goto end


:end
