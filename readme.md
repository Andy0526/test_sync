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

[回到顶部](#readme)

[API desc](#api-description)

[data数据说明](#data数据说明)

[block数据说明](#1-block数据说明)
