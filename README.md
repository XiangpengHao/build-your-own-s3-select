The source code of my blog: [Build your own S3-Select in 400 lines of Rust](https://blog.xiangpeng.systems/posts/build-s3-select/).

You may also want to checkout [LiquidCache](https://github.com/XiangpengHao/liquid-cache), a more feature-rich version of this project.

### Clone and install Rust

```bash
git clone https://github.com/XiangpengHao/build-your-own-s3-select.git

cd build-your-own-s3-select

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Run the server 

```bash
cargo run --bin server
```

### Run the client

```bash
cargo run --bin client
```

You should see the following output. 
Note the SQL to pushdown only contains filters and projection, i.e., no `DISTINCT` or `GROUP BY`.

```bash
SQL to run: 
-------
SELECT DISTINCT "city" FROM "aws-edge-locations" WHERE "country" = 'United States'
-------

SQL to pushdown: 
-------
SELECT "aws-edge-locations"."city" FROM "aws-edge-locations" WHERE ("aws-edge-locations"."country" = 'United States')
-------

+----------------+
| city           |
+----------------+
| Boston         |
| Chicago        |
| Portland       |
| New York       |
| Newark         |
| Detroit        |
...
```


### Lines of code

Note that we have more than 50 lines of imports.
```
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Rust                    2          484          431            1           52
```









