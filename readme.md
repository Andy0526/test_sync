# README

## Api description

URL                                   | Method | Desc
:------------------------------------ | :----- | :-------
/account/setpassword        | `POST` | 设置密码
/account/forgetpassword    | `POST` | 忘记密码重置密码
/v1.0/account/resetpassword | `POST` | 重置密码

## URL parameter

key      | Requried | type   | description
-------- | -------- | ------ | -----------
min_time | y        | string | 最新拉黑时间

## Response json

key  | Requried | type   | description
---- | -------- | ------ | -------------
code | y        | number | 返回状态码
data | y        | object | 返回的数据，内包含用户数据

### data数据说明

key       | Requried | type  | description
--------- | -------- | ----- | -----------
blocklist | y        | array | 拉黑列表

#### 1. block数据说明

key              | Requried | type   | description
---------------- | -------- | ------ | -----------
block_time       | y        | string | 拉黑时间
uid              | y        | number | uid
avatar_thumbnail | y        | string | 头像缩略图
name             | y        | string | 昵称

## 3. 接口列表

### 3.1 POST /account/setpassword

AUTH:True

请求参数

key      | Requried | type   | description
-------- | -------- | ------ | -----------
phone    | y        | string | 手机号码
password | y        | string | 密码
captcha  | y        | string | 验证码验证

### 3.2 POST /account/forgetpassword

AUTH:True

请求参数

key      | Requried | type   | description
-------- | -------- | ------ | -----------
phone    | y        | string | 手机号码
password | y        | string | 密码
captcha  | y        | string | 验证码验证

### 3.3 POST /v1.0/account/resetpassword

AUTH:True

请求参数

key          | Requried | type   | description
------------ | -------- | ------ | -----------
new_password | y        | string | 新密码
old_password | y        | string | 旧密码


[回到顶部](#readme)

[API desc](#api-description)

[data数据说明](#data数据说明)

[block数据说明](#1-block数据说明)

[resetpassword](#33-post-/v1.0/account/resetpassword)

[forgetpassword](#32-post-accountforgetpassword)
