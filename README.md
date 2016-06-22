### 部署安装

#### 依赖环境

+ 下载安装golang编译环境
    
    ```
    cd /usr/local
    
    git clone git@gitlab.jiuzhilan.net:wangxiaohui/go.git
    
    cd go/src
    
    ./all.bash
    
    export GOROOT=/usr/local/go
    
    export PATH=$PATH:$GOROOT/bin
    ```
    
+ 下载golang的第三方库

    ```
    cd /usr/local
    
    git clone git@gitlab.jiuzhilan.net:wangxiaohui/go-third-packages.git
    
    export GOPATH=$GOPATH:/usr/local/go-third-packages
    ```

#### 部署流程

```
cd /usr/local

git clone git@gitlab.jiuzhilan.net:wangxiaohui/dmp_bi_ap.git

export GOPATH=$GOPATH:/usr/local/dmp_bi_ap

cd dmp_bi_ap

sh install.sh

sh start.sh
```

