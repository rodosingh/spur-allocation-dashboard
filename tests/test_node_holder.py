import json, os, pathlib, subprocess, tempfile, unittest, time
SCRIPT=str(pathlib.Path(__file__).resolve().parents[1] / 'node_holder.sh')
MOCK=r'''#!/usr/bin/env python3
import os,sys,json,pathlib
p=pathlib.Path(os.environ['MOCK_ROOT']); name=pathlib.Path(sys.argv[0]).name; a=sys.argv[1:]
f=p/'jobs.json'; jobs=json.loads(f.read_text())
def save(): f.write_text(json.dumps(jobs))
if name=='getent': print('tester:x:1000:1000::'+str(p)+':/bin/bash')
elif name=='id': print('1000' if '-u' in a and '-un' not in a else 'tester')
elif name=='crontab':
 if (p/'cron_disabled').exists():
  print('You (tester) are not allowed to use this program (crontab)',file=sys.stderr);sys.exit(1)
 c=p/'cron'
 if a==['-l']:
  if not c.exists(): print('no crontab for tester',file=sys.stderr);sys.exit(1)
  print(c.read_text(),end='')
 else: c.write_text(sys.stdin.read())
elif name=='sinfo': print('node026' if '%N' in a else '1-00:00:00')
elif name=='spur':
 if a[:2]==['show','node']:
  if (p/'node_fail').exists(): sys.exit(1)
  capacities=json.loads((p/'nodes.json').read_text()) if (p/'nodes.json').exists() else {}
  print(capacities.get(a[2], 'NodeName='+a[2]+' State=IDLE CPUTot=236 CPUAlloc=0'))
 elif 'qos' in a:
  if (p/'qos_fail').exists(): sys.exit(1)
  print(f'{"Name":<30} {"Priority":<10} {"PreemptMode":<14} MaxWall')
  for q,pr in [('high-qos',100 if (p/'downgrade').exists() else 10000),('low-qos',100),('other-qos',10000)]: print(f'{q:<30} {pr:<10} {"off":<14} 1440')
 else:
  print('tester amd-test x amd-test '+('high-qos,low-qos,other-qos' if (p/'multi').exists() else 'high-qos,low-qos')+' high-qos')
  if (p/'duplicate_qos').exists(): print('tester amd-other x amd-other high-qos high-qos')
elif name=='squeue':
 if (p/'queue_fail').exists(): sys.exit(1)
 fmt=a[a.index('-o')+1]
 selected=list(jobs)
 if '-t' in a: selected=[j for j in selected if j['state']==a[a.index('-t')+1]]
 for j in selected:
  vals={'%i':str(j['id']),'%j':j['name'],'%T':j['state'],'%N':j['node'],'%r':'None','%q':j['qos'],'%D':'1','%a':'amd-test','%l':'1-00:00:00'}
  out=fmt
  for k,v in vals.items(): out=out.replace(k,v)
  print(out)
elif name=='sbatch':
 if (p/'fail_second').exists() and len(jobs)>0: sys.exit(1)
 if (p/'slow_submit').exists():
  import time
  (p/'submitting').touch();time.sleep(0.5)
 def val(k,d=''): return a[a.index(k)+1] if k in a else d
 jid=int((p/'next').read_text()) if (p/'next').exists() else 100
 (p/'next').write_text(str(jid+1))
 jobs.append({'id':jid,'name':val('-J'),'state':'PENDING','node':val('-w'),'qos':val('-q'),'dep':val('-d'),'args':a});save();print(jid)
elif name=='scancel':
 if (p/'cancel_fail').exists(): print('mock refusal',file=sys.stderr);sys.exit(1)
 jobs=[j for j in jobs if str(j['id']) not in a];save()
'''
class TestCLI(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix='nodeholder-test-');self.p=pathlib.Path(self.tmp.name)
  (self.p/'jobs.json').write_text('[]'); (self.p/'bin').mkdir()
  for n in ['id','getent','crontab','squeue','sbatch','scancel','spur','sinfo']:
   f=self.p/'bin'/n;f.write_text(MOCK);f.chmod(0o755)
  self.env=dict(os.environ,MOCK_ROOT=str(self.p),PATH=str(self.p/'bin')+':'+os.environ['PATH'],NODEHOLD_DIR=str(self.p/'state'),NODEHOLD_NAME='hold',SPUR_CONTROLLER_ADDR='mock',NODEHOLD_CHAIN='2',NODEHOLD_HEADROOM_MB='0')
 def tearDown(self): self.tmp.cleanup()
 def runcli(self,*args,ok=True,env=None):
  r=subprocess.run(['bash',SCRIPT,*args],env=env or self.env,text=True,capture_output=True,timeout=15)
  if ok:self.assertEqual(r.returncode,0,r.stdout+r.stderr)
  else:self.assertNotEqual(r.returncode,0,r.stdout+r.stderr)
  return r
 def jobs(self):return json.loads((self.p/'jobs.json').read_text())
 def start(self,*args):return self.runcli('-n','work',*args,'start')
 def test_lifecycle(self):
  self.start('-q','high-qos','-w','node026')
  self.assertTrue((self.p/'state/hold-work.race').exists())
  cron=(self.p/'cron').read_text();self.assertIn(SCRIPT,cron);self.assertNotIn('node_holder_tick.sh',cron)
  j=self.jobs();self.assertEqual(j[0]['node'],'node026');j[0]['state']='RUNNING';(self.p/'jobs.json').write_text(json.dumps(j))
  e=dict(self.env,NODEHOLD_NAME='hold-work');self.runcli('tick',env=e)
  j=self.jobs();self.assertEqual(len(j),3);self.assertEqual(j[1]['dep'],'afterany:100');self.assertEqual(j[2]['dep'],'afterany:101')
  self.runcli('-n','work','release');self.assertEqual(self.jobs(),[])
  self.assertNotIn('NODEHOLD_NAME=hold-work ',(self.p/'cron').read_text())
  self.assertTrue((self.p/'state/hold-work.released').exists())
  self.runcli('tick',env=e,ok=False);self.assertEqual(self.jobs(),[])
  self.start('-q','high-qos');self.assertEqual(len(self.jobs()),1)
 def test_policy(self):
  self.start('-q','high-qos')
  self.runcli('-n','low','-q','low-qos','start',ok=False)
  self.assertEqual(len(self.jobs()),1)
  self.runcli('-n','override','-q','low-qos','--any-qos','start')
  self.assertEqual(len(self.jobs()),2)
 def test_adopt_policy(self):
  (self.p/'jobs.json').write_text(json.dumps([dict(id=1,name='external',state='RUNNING',node='node026',qos='high-qos')]))
  self.runcli('-n','adopted','-q','low-qos','adopt','1',ok=False)
  self.assertEqual(len(self.jobs()),1)
  self.runcli('-n','adopted','-q','high-qos','adopt','1')
  self.assertEqual(len(self.jobs()),3)
 def test_release_failure_preserves_state(self):
  self.start('-q','high-qos');(self.p/'cancel_fail').touch()
  self.runcli('-n','work','release',ok=False)
  self.assertEqual(len(self.jobs()),1);self.assertTrue((self.p/'state/hold-work.conf').exists())
  (self.p/'cancel_fail').unlink();self.runcli('-n','work','release');self.assertEqual(self.jobs(),[])
 def test_release_queue_failure(self):
  self.start('-q','high-qos');(self.p/'queue_fail').touch()
  self.runcli('-n','work','release',ok=False)
  self.assertTrue((self.p/'state/hold-work.conf').exists())
 def test_pin_recovery(self):
  self.start('-q','high-qos','-w','node026');(self.p/'jobs.json').write_text('[]')
  self.runcli('tick',env=dict(self.env,NODEHOLD_NAME='hold-work'))
  self.assertEqual(self.jobs()[0]['node'],'node026')
 def test_shared_race_release(self):
  self.start('-q','high-qos')
  f=self.p/'state/hold.race';f.write_text('hold-work|amd-test|high-qos|off|86400\nhold-other|amd-test|high-qos|off|86400\n')
  self.runcli('-n','work','release');self.assertNotIn('hold-work|',f.read_text());self.assertIn('hold-other|',f.read_text())
 def test_tend_idempotent(self):
  self.start('-q','high-qos');self.runcli('-n','work','tend');self.runcli('-n','work','tend')
  self.assertEqual((self.p/'cron').read_text().count('NODEHOLD_NAME=hold-work '),1)
 def test_race(self):
  self.runcli('race');j=self.jobs();self.assertEqual(len(j),1);self.assertEqual(j[0]['qos'],'high-qos')
  self.assertTrue((self.p/'state/hold.race').exists());self.assertIn('NODEHOLD_NAME=hold ',(self.p/'cron').read_text())
 def test_unknown_policy_refused(self):
  (self.p/'qos_fail').touch()
  self.runcli('-n','work','-q','high-qos','start',ok=False)
  self.assertEqual(self.jobs(),[])
 def test_topup_rechecks_policy(self):
  self.start('-q','high-qos');j=self.jobs();j[0].update(state='RUNNING',node='node026');(self.p/'jobs.json').write_text(json.dumps(j))
  (self.p/'downgrade').touch()
  self.runcli('-n','work','topup',ok=False);self.assertEqual(len(self.jobs()),1)
 def test_tend_repairs_missing_race(self):
  self.start('-q','high-qos');(self.p/'state/hold-work.race').unlink()
  self.runcli('-n','work','tend');self.assertTrue((self.p/'state/hold-work.race').exists())
 def test_released_shared_member_never_resubmits(self):
  self.start('-q','high-qos');self.runcli('-n','work','release')
  (self.p/'state/hold.race').write_text('hold-work|amd-test|high-qos|off|86400\n')
  self.runcli('tick');self.assertEqual(self.jobs(),[])
 def test_cron_preserves_other_chain(self):
  self.start('-q','high-qos');self.runcli('-n','other','-q','high-qos','start')
  self.runcli('-n','work','release')
  c=(self.p/'cron').read_text();self.assertIn('NODEHOLD_NAME=hold-other ',c);self.assertNotIn('NODEHOLD_NAME=hold-work ',c)
 def test_race_winner_retires_loser(self):
  (self.p/'multi').touch();self.runcli('race');j=self.jobs();self.assertEqual(len(j),2)
  j[0].update(state='RUNNING',node='node026');loser=j[1]['name'];(self.p/'jobs.json').write_text(json.dumps(j))
  self.runcli('tick');self.assertTrue(all(x['name']!=loser for x in self.jobs()))
  self.assertTrue((self.p/f'state/{loser}.released').exists());self.runcli('tick')
  self.assertTrue(all(x['name']!=loser for x in self.jobs()))
 def test_start_release_serialized(self):
  (self.p/'slow_submit').touch()
  proc=subprocess.Popen(['bash',SCRIPT,'-n','work','-q','high-qos','start'],env=self.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  try:
   until=time.monotonic()+5
   while not (self.p/'submitting').exists() and time.monotonic()<until: time.sleep(.01)
   self.assertTrue((self.p/'submitting').exists())
   self.runcli('-n','work','release')
   out,err=proc.communicate(timeout=5);self.assertEqual(proc.returncode,0,out+err);self.assertEqual(self.jobs(),[])
  finally:
   if proc.poll() is None:proc.kill();proc.communicate()
 def test_start_refuses_queue_outage(self):
  (self.p/'queue_fail').touch()
  self.runcli('-n','work','-q','high-qos','start',ok=False)
  self.assertEqual(self.jobs(),[])
 def test_partial_gpus(self):
  self.start('-q','high-qos','-g','2');j=self.jobs()[0]
  self.assertIn('--gres=gpu:2',j['args']);self.assertNotIn('--exclusive',j['args'])
 def test_cpu_share_and_successors(self):
  self.start('-q','high-qos','-g','4')
  j=self.jobs();self.assertEqual(j[0]['args'][j[0]['args'].index('-c')+1],'118')
  j[0].update(state='RUNNING',node='node026');(self.p/'jobs.json').write_text(json.dumps(j))
  self.runcli('-n','work','topup')
  for j in self.jobs(): self.assertEqual(j['args'][j['args'].index('-c')+1],'118')
 def test_cpu_explicit_override(self):
  (self.p/'node_fail').touch()
  self.start('-q','high-qos','-g','4','-c','12')
  j=self.jobs()[0];self.assertEqual(j['args'][j['args'].index('-c')+1],'12')
 def test_cpu_adopt(self):
  (self.p/'jobs.json').write_text(json.dumps([dict(id=1,name='external',state='RUNNING',node='node026',qos='high-qos')]))
  self.runcli('-n','adopted','-q','high-qos','-g','2','adopt','1')
  for j in self.jobs()[1:]: self.assertEqual(j['args'][j['args'].index('-c')+1],'59')
 def test_cpu_lookup_failure(self):
  (self.p/'node_fail').touch()
  self.runcli('-n','work','-q','high-qos','-g','4','start',ok=False)
  self.assertEqual(self.jobs(),[])
 def test_cpu_invalid_environment(self):
  self.runcli('-n','work','start',env=dict(self.env,NODEHOLD_CPUS='bad'),ok=False)
  self.assertEqual(self.jobs(),[])
 def prepare_lost_pin(self):
  self.start('-q','high-qos','-g','4','-w','node026','--pair','half2')
  (self.p/'state/hold-work.state').write_text('node=node026\n')
  (self.p/'nodes.json').write_text(json.dumps({'node026':'State=ALLOCATED CPUTot=236 CPUAlloc=236','node027':'State=MIXED CPUTot=236 CPUAlloc=118'}))
  j=self.jobs();j.append(dict(id=99,name='hold-half2',state='RUNNING',node='node027',qos='high-qos'))
  (self.p/'jobs.json').write_text(json.dumps(j))
 def test_pair_follows_partner(self):
  self.prepare_lost_pin()
  self.assertIn('PAIR=hold-half2',(self.p/'state/hold-work.conf').read_text())
  self.runcli('tick',env=dict(self.env,NODEHOLD_NAME='hold-work'))
  own=[j for j in self.jobs() if j['name']=='hold-work']
  self.assertEqual(len(own),1);self.assertEqual(own[0]['node'],'node027')
 def test_repin_lookup_failure_keeps_queue(self):
  self.prepare_lost_pin();before=self.jobs();(self.p/'node_fail').touch()
  self.runcli('tick',env=dict(self.env,NODEHOLD_NAME='hold-work'))
  self.assertEqual(self.jobs(),before)
 def test_repin_unknown_capacity_keeps_queue(self):
  self.prepare_lost_pin();before=self.jobs()
  (self.p/'nodes.json').write_text(json.dumps({'node026':'State=ALLOCATED'}))
  self.runcli('tick',env=dict(self.env,NODEHOLD_NAME='hold-work'))
  self.assertEqual(self.jobs(),before)
 def test_repin_optout_persisted(self):
  self.env['NODEHOLD_REPIN']='0';self.prepare_lost_pin();before=self.jobs()
  env=dict(self.env,NODEHOLD_NAME='hold-work');env.pop('NODEHOLD_REPIN')
  self.assertIn('REPIN=0',(self.p/'state/hold-work.conf').read_text())
  self.runcli('tick',env=env);self.assertEqual(self.jobs(),before)
 def test_invalid_gpu_and_options(self):
  self.runcli('-n','work','-g','9','start',ok=False)
  self.runcli('start','--dasy','3',ok=False);self.assertEqual(self.jobs(),[])
 def test_multinode_release_group(self):
  self.runcli('-n','fleet','start','-N','3','-q','high-qos')
  self.assertEqual({j['name'] for j in self.jobs()},{'hold-fleet-1','hold-fleet-2','hold-fleet-3'})
  self.runcli('-n','fleet','release','-N','3');self.assertEqual(self.jobs(),[])
 def test_multinode_shared_refused(self):
  self.runcli('start','-N','2','-g','2',ok=False);self.assertEqual(self.jobs(),[])
  self.runcli('start','-N','2','-g','2','--exclusive','-q','high-qos');self.assertEqual(len(self.jobs()),2)
 def test_partial_multinode_reports_failure(self):
  (self.p/'fail_second').touch()
  self.runcli('start','-N','2','-q','high-qos',ok=False);self.assertEqual(len(self.jobs()),1)
 def test_duration_rounds_actual_sleep(self):
  self.start('-q','high-qos','-days','3')
  self.assertIn('CHAIN=4',(self.p/'state/hold-work.conf').read_text())
 def test_time_and_chain(self):
  self.start('-q','high-qos','--time','06:00:00','--chain','3')
  j=self.jobs()[0];self.assertEqual(j['args'][j['args'].index('-t')+1],'06:00:00')
  self.assertIn('CHAIN=3',(self.p/'state/hold-work.conf').read_text())
 def test_adopt_duration(self):
  (self.p/'jobs.json').write_text(json.dumps([dict(id=1,name='external',state='RUNNING',node='node026',qos='high-qos')]))
  self.runcli('-n','adopted','-q','high-qos','--days','3','-g','2','adopt','1')
  self.assertEqual(len(self.jobs()),5);self.assertNotIn('--exclusive',self.jobs()[1]['args'])
 def test_finite_expiry(self):
  self.start('-q','high-qos','--for-hours','2')
  f=self.p/'state/hold-work.conf';conf=f.read_text()
  self.assertNotIn('EXPIRES_AT=0',conf)
  import re
  f.write_text(re.sub(r'EXPIRES_AT=\d+','EXPIRES_AT=1',conf))
  self.runcli('tick',env=dict(self.env,NODEHOLD_NAME='hold-work'))
  self.assertEqual(self.jobs(),[]);self.assertTrue((self.p/'state/hold-work.released').exists())
 def test_release_all_race_only(self):
  (self.p/'state').mkdir();(self.p/'state/hold.race').write_text('hold-orphan|amd-test|high-qos|off|86400\n')
  self.runcli('release','--all');self.assertTrue((self.p/'state/hold-orphan.released').exists())
 def test_pool_totals_not_duplicated(self):
  self.start('-q','high-qos');(self.p/'duplicate_qos').touch()
  r=self.runcli('pools');self.assertIn('0 running job(s), 1 queued',r.stdout)
 def test_exact_full_name_disambiguates_nested_chain_names(self):
  self.runcli('-n','a','-q','high-qos','start')
  self.runcli('-n','a-b','-q','high-qos','start')
  env=dict(self.env,NODEHOLD_NAME='hold-a',NODEHOLD_CHAIN_FULL_NAME='hold-a')
  self.runcli('release',env=env)
  self.assertEqual({job['name'] for job in self.jobs()},{'hold-a-b'})
 def test_disabled_crontab_still_submits_and_releases(self):
  # A login node that denies crontab must not block submission or release:
  # the chain runs without local tending instead of aborting.
  (self.p/'cron_disabled').touch()
  env=dict(self.env,NODEHOLD_ARM='0')
  r=self.runcli('-n','work','-q','high-qos','start',env=env)
  self.assertEqual(len(self.jobs()),1)
  self.assertIn('not tended from here',r.stdout)
  self.runcli('-n','work','release',env=env)
  self.assertEqual(self.jobs(),[])
 def test_release_refuses_a_name_that_is_not_a_chain(self):
  # A full chain name passed to -n is re-prefixed (hold + hold-work), so it
  # names a phantom. Releasing it must fail loudly, not tombstone a non-chain.
  r=self.runcli('-n','hold-work','release',ok=False)
  self.assertIn('no chain named',r.stdout+r.stderr)
  self.assertFalse(list((self.p/'state').glob('*.released')))
 def test_pools_json_is_structured_and_keeps_duplicate_associations(self):
  (self.p/'duplicate_qos').touch()
  payload=json.loads(self.runcli('pools-json').stdout)
  self.assertEqual(payload['schemaVersion'],1)
  self.assertEqual(payload['bestPool'],{'account':'amd-test','qos':'high-qos'})
  self.assertEqual(
   {(pool['account'],pool['qos']) for pool in payload['pools']},
   {('amd-test','high-qos'),('amd-test','low-qos'),('amd-other','high-qos')},
  )
 def test_status_json_reports_saved_chain_and_jobs(self):
  self.start('-q','high-qos','-g','4','--chain','2')
  jobs=self.jobs();jobs[0].update(state='RUNNING',node='node026')
  (self.p/'jobs.json').write_text(json.dumps(jobs))
  payload=json.loads(self.runcli('status-json').stdout)
  self.assertEqual(payload['schemaVersion'],1)
  self.assertEqual(len(payload['chains']),1)
  chain=payload['chains'][0]
  self.assertEqual(chain['name'],'hold-work')
  self.assertEqual(chain['gpus'],4)
  self.assertEqual(chain['cpus'],118)
  self.assertFalse(chain['exclusive'])
  self.assertEqual(chain['runningNode'],'node026')
  self.assertEqual(chain['jobs'][0]['id'],'100')
 def test_doctor_json_reports_health_and_chain_membership(self):
  self.start('-q','high-qos')
  payload=json.loads(self.runcli('doctor-json').stdout)
  self.assertTrue(payload['scheduler']['answering'])
  self.assertTrue(payload['nameService']['resolvesUser'])
  self.assertTrue(payload['home']['writable'])
  self.assertEqual(payload['cron']['tendedChains'],1)
  chain=next(item for item in payload['chains'] if item['name']=='hold-work')
  self.assertTrue(chain['accountGranted'])
  self.assertTrue(chain['tended'])
if __name__=='__main__': unittest.main(verbosity=2)
