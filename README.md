# 6.5840
通过

lab1  mapreduce

lab2a  election

lab2b  log

lab2c  persistence

lab2d  log compaction

lab2还是比较复杂的，代码量也比较大，很多细节容易出错，例如各种下标：nextIndex、加了快照之后的下标等。此外，当Persistent state修改后，一定要调用persist()存起来。

也遇到过一些bug

lab2a

一开始一直过不了，这个是因为给channel发消息，可能之前的消息还没被消费掉，导致了阻塞。因此发消息可以用select判断一下。

lab2b

感觉lab2b和lab2c需要写的反而不多，主体都在lab2a完成了，只需要把相应的逻辑加上去。

lab2c
Figure 8 (unreliable) ...，fail to reach agreement。这个可以参考https://github.com/springfieldking/mit-6.824-golabs-2018/issues/1，给出了很多种可能和解决方案。

lab2d

snapshots basic ...可能在applymsg中给applych发消息的时候snapshot被调用了，导致死锁，最后整个系统就只剩1个server了。因此给applych发消息的时候要释放锁。

还遇到过snapshot decode error，解决办法就是persist和readpersist要按要求写，Persistent state修改后，一定要调用persist()存起来。

apply error 这个问题在我另开一个go routine进行applymsg之后就解决了，保证了每一个server只有一个go routine在给applych发送消息，因此不会出现out of order。并且改成这样之后，lab2d的snapshots basic从40s降到了10s内。

总的来说，调bug就是在需要的地方加上一个printf，把当前的状态打印出来，然后再分析哪里出问题了。
