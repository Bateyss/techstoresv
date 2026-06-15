var Ck=Object.defineProperty,Dk=Object.defineProperties;var wk=Object.getOwnPropertyDescriptors;var iu=Object.getOwnPropertySymbols;var hC=Object.prototype.hasOwnProperty,gC=Object.prototype.propertyIsEnumerable;var pC=(t,n,e)=>n in t?Ck(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,D=(t,n)=>{for(var e in n||={})hC.call(n,e)&&pC(t,e,n[e]);if(iu)for(var e of iu(n))gC.call(n,e)&&pC(t,e,n[e]);return t},fe=(t,n)=>Dk(t,wk(n));var vC=(t,n)=>{var e={};for(var i in t)hC.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&iu)for(var i of iu(t))n.indexOf(i)<0&&gC.call(t,i)&&(e[i]=t[i]);return e};var Zt=null,ru=!1,Nh=1,xk=null,bt=Symbol("SIGNAL");function ce(t){let n=Zt;return Zt=t,n}function ou(){return Zt}var co={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function uo(t){if(ru)throw new Error("");if(Zt===null)return;Zt.consumerOnSignalRead(t);let n=Zt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Zt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Zt.producers,e!==void 0&&e.producer===t)){Zt.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Zt&&(!i||Sk(r,Zt)))return;let o=ts(Zt),a={producer:t,consumer:Zt,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};Zt.producersTail=a,n!==void 0?n.nextProducer=a:Zt.producers=a,o&&CC(t,a)}function _C(){Nh++}function na(t){if(!(ts(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Nh)){if(!t.producerMustRecompute(t)&&!es(t)){Ja(t);return}t.producerRecomputeValue(t),Ja(t)}}function Ph(t){if(t.consumers===void 0)return;let n=ru;ru=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||Ek(i)}}finally{ru=n}}function Fh(){return Zt?.consumerAllowSignalWrites!==!1}function Ek(t){t.dirty=!0,Ph(t),t.consumerMarkedDirty?.(t)}function Ja(t){t.dirty=!1,t.lastCleanEpoch=Nh}function Nr(t){return t&&yC(t),ce(t)}function yC(t){t.producersTail=void 0,t.recomputing=!0}function fo(t,n){ce(n),t&&bC(t)}function bC(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(ts(t))do e=Lh(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function es(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(na(e),i!==e.version))return!0}return!1}function mo(t){if(ts(t)){let n=t.producers;for(;n!==void 0;)n=Lh(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function CC(t,n){let e=t.consumersTail,i=ts(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)CC(r.producer,r)}function Lh(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!ts(n)){let o=n.producers;for(;o!==void 0;)o=Lh(o)}return e}function ts(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function kl(t){xk?.(t)}function Sk(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Ol(t,n){return Object.is(t,n)}function Nl(t,n){let e=Object.create(Mk);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(na(e),uo(e),e.value===or)throw e.error;return e.value};return i[bt]=e,kl(e),i}var ea=Symbol("UNSET"),ta=Symbol("COMPUTING"),or=Symbol("ERRORED"),Mk=fe(D({},co),{value:ea,dirty:!0,error:null,equal:Ol,kind:"computed",producerMustRecompute(t){return t.value===ea||t.value===ta},producerRecomputeValue(t){if(t.value===ta)throw new Error("");let n=t.value;t.value=ta;let e=Nr(t),i,r=!1;try{i=t.computation(),ce(null),r=n!==ea&&n!==or&&i!==or&&t.equal(n,i)}catch(o){i=or,t.error=o}finally{fo(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Ik(){throw new Error}var DC=Ik;function wC(t){DC(t)}function Vh(t){DC=t}var Tk=null;function jh(t,n){let e=Object.create(Pl);e.value=t,n!==void 0&&(e.equal=n);let i=()=>xC(e);return i[bt]=e,kl(e),[i,a=>ia(e,a),a=>au(e,a)]}function xC(t){return uo(t),t.value}function ia(t,n){Fh()||wC(t),t.equal(t.value,n)||(t.value=n,Ak(t))}function au(t,n){Fh()||wC(t),ia(t,n(t.value))}var Pl=fe(D({},co),{equal:Ol,value:void 0,kind:"signal"});function Ak(t){t.version++,_C(),Ph(t),Tk?.(t)}var Bh=fe(D({},co),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Hh(t){if(t.dirty=!1,t.version>0&&!es(t))return;t.version++;let n=Nr(t);try{t.cleanup(),t.fn()}finally{fo(t,n)}}function Ee(t){return typeof t=="function"}function ns(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var su=ns(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function ra(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var me=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(Ee(i))try{i()}catch(o){n=o instanceof su?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{EC(o)}catch(a){n=n??[],a instanceof su?n=[...n,...a.errors]:n.push(a)}}if(n)throw new su(n)}}add(n){var e;if(n&&n!==this)if(this.closed)EC(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&ra(e,n)}remove(n){let{_finalizers:e}=this;e&&ra(e,n),n instanceof t&&n._removeParent(this)}};me.EMPTY=(()=>{let t=new me;return t.closed=!0,t})();var Uh=me.EMPTY;function lu(t){return t instanceof me||t&&"closed"in t&&Ee(t.remove)&&Ee(t.add)&&Ee(t.unsubscribe)}function EC(t){Ee(t)?t():t.unsubscribe()}var Ti={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var is={setTimeout(t,n,...e){let{delegate:i}=is;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=is;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function cu(t){is.setTimeout(()=>{let{onUnhandledError:n}=Ti;if(n)n(t);else throw t})}function Fl(){}var SC=zh("C",void 0,void 0);function MC(t){return zh("E",void 0,t)}function IC(t){return zh("N",t,void 0)}function zh(t,n,e){return{kind:t,value:n,error:e}}var oa=null;function rs(t){if(Ti.useDeprecatedSynchronousErrorHandling){let n=!oa;if(n&&(oa={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=oa;if(oa=null,e)throw i}}else t()}function TC(t){Ti.useDeprecatedSynchronousErrorHandling&&oa&&(oa.errorThrown=!0,oa.error=t)}var aa=class extends me{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,lu(n)&&n.add(this)):this.destination=Ok}static create(n,e,i){return new Pr(n,e,i)}next(n){this.isStopped?Gh(IC(n),this):this._next(n)}error(n){this.isStopped?Gh(MC(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Gh(SC,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Rk=Function.prototype.bind;function $h(t,n){return Rk.call(t,n)}var Wh=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){du(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){du(i)}else du(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){du(e)}}},Pr=class extends aa{constructor(n,e,i){super();let r;if(Ee(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Ti.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&$h(n.next,o),error:n.error&&$h(n.error,o),complete:n.complete&&$h(n.complete,o)}):r=n}this.destination=new Wh(r)}};function du(t){Ti.useDeprecatedSynchronousErrorHandling?TC(t):cu(t)}function kk(t){throw t}function Gh(t,n){let{onStoppedNotification:e}=Ti;e&&is.setTimeout(()=>e(t,n))}var Ok={closed:!0,next:Fl,error:kk,complete:Fl};var os=typeof Symbol=="function"&&Symbol.observable||"@@observable";function $n(t){return t}function qh(...t){return Yh(t)}function Yh(t){return t.length===0?$n:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ye=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=Pk(e)?e:new Pr(e,i,r);return rs(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=AC(i),new i((r,o)=>{let a=new Pr({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[os](){return this}pipe(...e){return Yh(e)(this)}toPromise(e){return e=AC(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function AC(t){var n;return(n=t??Ti.Promise)!==null&&n!==void 0?n:Promise}function Nk(t){return t&&Ee(t.next)&&Ee(t.error)&&Ee(t.complete)}function Pk(t){return t&&t instanceof aa||Nk(t)&&lu(t)}function Qh(t){return Ee(t?.lift)}function we(t){return n=>{if(Qh(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function be(t,n,e,i,r){return new Zh(t,n,e,i,r)}var Zh=class extends aa{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};function RC(){return we((t,n)=>{let e=null;t._refCount++;let i=be(n,void 0,void 0,void 0,()=>{if(!t||t._refCount<=0||0<--t._refCount){e=null;return}let r=t._connection,o=e;e=null,r&&(!o||r===o)&&r.unsubscribe(),n.unsubscribe()});t.subscribe(i),i.closed||(e=t.connect())})}var Ll=class extends ye{constructor(n,e){super(),this.source=n,this.subjectFactory=e,this._subject=null,this._refCount=0,this._connection=null,Qh(n)&&(this.lift=n.lift)}_subscribe(n){return this.getSubject().subscribe(n)}getSubject(){let n=this._subject;return(!n||n.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:n}=this;this._subject=this._connection=null,n?.unsubscribe()}connect(){let n=this._connection;if(!n){n=this._connection=new me;let e=this.getSubject();n.add(this.source.subscribe(be(e,void 0,()=>{this._teardown(),e.complete()},i=>{this._teardown(),e.error(i)},()=>this._teardown()))),n.closed&&(this._connection=null,n=me.EMPTY)}return n}refCount(){return RC()(this)}};var as={schedule(t){let n=requestAnimationFrame,e=cancelAnimationFrame,{delegate:i}=as;i&&(n=i.requestAnimationFrame,e=i.cancelAnimationFrame);let r=n(o=>{e=void 0,t(o)});return new me(()=>e?.(r))},requestAnimationFrame(...t){let{delegate:n}=as;return(n?.requestAnimationFrame||requestAnimationFrame)(...t)},cancelAnimationFrame(...t){let{delegate:n}=as;return(n?.cancelAnimationFrame||cancelAnimationFrame)(...t)},delegate:void 0};var kC=ns(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var k=(()=>{class t extends ye{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new uu(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new kC}next(e){rs(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){rs(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){rs(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Uh:(this.currentObservers=null,o.push(e),new me(()=>{this.currentObservers=null,ra(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ye;return e.source=this,e}}return t.create=(n,e)=>new uu(n,e),t})(),uu=class extends k{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Uh}};var Ct=class extends k{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Vl={now(){return(Vl.delegate||Date).now()},delegate:void 0};var po=class extends k{constructor(n=1/0,e=1/0,i=Vl){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var fu=class extends me{constructor(n,e){super()}schedule(n,e=0){return this}};var jl={setInterval(t,n,...e){let{delegate:i}=jl;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=jl;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var ho=class extends fu{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return jl.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&jl.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,ra(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Fk=1,Kh,Xh={};function OC(t){return t in Xh?(delete Xh[t],!0):!1}var NC={setImmediate(t){let n=Fk++;return Xh[n]=!0,Kh||(Kh=Promise.resolve()),Kh.then(()=>OC(n)&&t()),n},clearImmediate(t){OC(t)}};var{setImmediate:Lk,clearImmediate:Vk}=NC,Bl={setImmediate(...t){let{delegate:n}=Bl;return(n?.setImmediate||Lk)(...t)},clearImmediate(t){let{delegate:n}=Bl;return(n?.clearImmediate||Vk)(t)},delegate:void 0};var mu=class extends ho{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=Bl.setImmediate(n.flush.bind(n,void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(Bl.clearImmediate(e),n._scheduled===e&&(n._scheduled=void 0))}};var ss=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};ss.now=Vl.now;var go=class extends ss{constructor(n,e=ss.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var pu=class extends go{flush(n){this._active=!0;let e=this._scheduled;this._scheduled=void 0;let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var hu=new pu(mu);var Hl=new go(ho),PC=Hl;var gu=class extends ho{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=as.requestAnimationFrame(()=>n.flush(void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&e===n._scheduled&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(as.cancelAnimationFrame(e),n._scheduled=void 0)}};var vu=class extends go{flush(n){this._active=!0;let e;n?e=n.id:(e=this._scheduled,this._scheduled=void 0);let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var _u=new vu(gu);var ft=new ye(t=>t.complete());function yu(t){return t&&Ee(t.schedule)}function Jh(t){return t[t.length-1]}function bu(t){return Ee(Jh(t))?t.pop():void 0}function ar(t){return yu(Jh(t))?t.pop():void 0}function FC(t,n){return typeof Jh(t)=="number"?t.pop():n}function VC(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(u){try{c(i.next(u))}catch(m){a(m)}}function l(u){try{c(i.throw(u))}catch(m){a(m)}}function c(u){u.done?o(u.value):r(u.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function LC(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function sa(t){return this instanceof sa?(this.v=t,this):new sa(t)}function jC(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(_){return function(b){return Promise.resolve(b).then(_,m)}}function s(_,b){i[_]&&(r[_]=function(A){return new Promise(function(R,L){o.push([_,A,R,L])>1||l(_,A)})},b&&(r[_]=b(r[_])))}function l(_,b){try{c(i[_](b))}catch(A){v(o[0][3],A)}}function c(_){_.value instanceof sa?Promise.resolve(_.value.v).then(u,m):v(o[0][2],_)}function u(_){l("next",_)}function m(_){l("throw",_)}function v(_,b){_(b),o.shift(),o.length&&l(o[0][0],o[0][1])}}function BC(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof LC=="function"?LC(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var Cu=t=>t&&typeof t.length=="number"&&typeof t!="function";function Du(t){return Ee(t?.then)}function wu(t){return Ee(t[os])}function xu(t){return Symbol.asyncIterator&&Ee(t?.[Symbol.asyncIterator])}function Eu(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function jk(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Su=jk();function Mu(t){return Ee(t?.[Su])}function Iu(t){return jC(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield sa(e.read());if(r)return yield sa(void 0);yield yield sa(i)}}finally{e.releaseLock()}})}function Tu(t){return Ee(t?.getReader)}function et(t){if(t instanceof ye)return t;if(t!=null){if(wu(t))return Bk(t);if(Cu(t))return Hk(t);if(Du(t))return Uk(t);if(xu(t))return HC(t);if(Mu(t))return zk(t);if(Tu(t))return $k(t)}throw Eu(t)}function Bk(t){return new ye(n=>{let e=t[os]();if(Ee(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Hk(t){return new ye(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Uk(t){return new ye(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,cu)})}function zk(t){return new ye(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function HC(t){return new ye(n=>{Gk(t,n).catch(e=>n.error(e))})}function $k(t){return HC(Iu(t))}function Gk(t,n){var e,i,r,o;return VC(this,void 0,void 0,function*(){try{for(e=BC(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Rn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Au(t,n=0){return we((e,i)=>{e.subscribe(be(i,r=>Rn(i,t,()=>i.next(r),n),()=>Rn(i,t,()=>i.complete(),n),r=>Rn(i,t,()=>i.error(r),n)))})}function Ru(t,n=0){return we((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function UC(t,n){return et(t).pipe(Ru(n),Au(n))}function zC(t,n){return et(t).pipe(Ru(n),Au(n))}function $C(t,n){return new ye(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function GC(t,n){return new ye(e=>{let i;return Rn(e,n,()=>{i=t[Su](),Rn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>Ee(i?.return)&&i.return()})}function ku(t,n){if(!t)throw new Error("Iterable cannot be null");return new ye(e=>{Rn(e,n,()=>{let i=t[Symbol.asyncIterator]();Rn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function WC(t,n){return ku(Iu(t),n)}function qC(t,n){if(t!=null){if(wu(t))return UC(t,n);if(Cu(t))return $C(t,n);if(Du(t))return zC(t,n);if(xu(t))return ku(t,n);if(Mu(t))return GC(t,n);if(Tu(t))return WC(t,n)}throw Eu(t)}function st(t,n){return n?qC(t,n):et(t)}function X(...t){let n=ar(t);return st(t,n)}function Ul(t,n){let e=Ee(t)?t:()=>t,i=r=>r.error(e());return new ye(n?r=>n.schedule(i,0,r):i)}function la(t){return!!t&&(t instanceof ye||Ee(t.lift)&&Ee(t.subscribe))}var ca=ns(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function YC(t){return t instanceof Date&&!isNaN(t)}function pe(t,n){return we((e,i)=>{let r=0;e.subscribe(be(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:Wk}=Array;function qk(t,n){return Wk(n)?t(...n):t(n)}function Ou(t){return pe(n=>qk(t,n))}var{isArray:Yk}=Array,{getPrototypeOf:Qk,prototype:Zk,keys:Kk}=Object;function Nu(t){if(t.length===1){let n=t[0];if(Yk(n))return{args:n,keys:null};if(Xk(n)){let e=Kk(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function Xk(t){return t&&typeof t=="object"&&Qk(t)===Zk}function Pu(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function sr(...t){let n=ar(t),e=bu(t),{args:i,keys:r}=Nu(t);if(i.length===0)return st([],n);let o=new ye(Jk(i,n,r?a=>Pu(r,a):$n));return e?o.pipe(Ou(e)):o}function Jk(t,n,e=$n){return i=>{QC(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)QC(n,()=>{let c=st(t[l],n),u=!1;c.subscribe(be(i,m=>{o[l]=m,u||(u=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function QC(t,n,e){t?Rn(e,t,n):n()}function ZC(t,n,e,i,r,o,a,s){let l=[],c=0,u=0,m=!1,v=()=>{m&&!l.length&&!c&&n.complete()},_=A=>c<i?b(A):l.push(A),b=A=>{o&&n.next(A),c++;let R=!1;et(e(A,u++)).subscribe(be(n,L=>{r?.(L),o?_(L):n.next(L)},()=>{R=!0},void 0,()=>{if(R)try{for(c--;l.length&&c<i;){let L=l.shift();a?Rn(n,a,()=>b(L)):b(L)}v()}catch(L){n.error(L)}}))};return t.subscribe(be(n,_,()=>{m=!0,v()})),()=>{s?.()}}function Kt(t,n,e=1/0){return Ee(n)?Kt((i,r)=>pe((o,a)=>n(i,o,r,a))(et(t(i,r))),e):(typeof n=="number"&&(e=n),we((i,r)=>ZC(i,r,t,e)))}function vo(t=1/0){return Kt($n,t)}function KC(){return vo(1)}function _o(...t){return KC()(st(t,ar(t)))}function Ai(t){return new ye(n=>{et(t()).subscribe(n)})}function zl(...t){let n=bu(t),{args:e,keys:i}=Nu(t),r=new ye(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let u=0;u<a;u++){let m=!1;et(e[u]).subscribe(be(o,v=>{m||(m=!0,c--),s[u]=v},()=>l--,void 0,()=>{(!l||!m)&&(c||o.next(i?Pu(i,s):s),o.complete())}))}});return n?r.pipe(Ou(n)):r}function XC(t=0,n,e=PC){let i=-1;return n!=null&&(yu(n)?e=n:i=n),new ye(r=>{let o=YC(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function sn(...t){let n=ar(t),e=FC(t,1/0),i=t;return i.length?i.length===1?et(i[0]):vo(e)(st(i,n)):ft}function Ie(t,n){return we((e,i)=>{let r=0;e.subscribe(be(i,o=>t.call(n,o,r++)&&i.next(o)))})}function JC(t){return we((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(be(e,c=>{i=!0,r=c,o||et(t(c)).subscribe(o=be(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function ls(t,n=Hl){return JC(()=>XC(t,n))}function yo(t){return we((n,e)=>{let i=null,r=!1,o;i=n.subscribe(be(e,void 0,void 0,a=>{o=et(t(a,yo(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function da(t,n){return Ee(n)?Kt(t,n,1):Kt(t,1)}function $l(t,n=Hl){return we((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}s()}e.subscribe(be(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function eD(t){return we((n,e)=>{let i=!1;n.subscribe(be(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function lt(t){return t<=0?()=>ft:we((n,e)=>{let i=0;n.subscribe(be(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Fu(t,n=$n){return t=t??e1,we((e,i)=>{let r,o=!0;e.subscribe(be(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function e1(t,n){return t===n}function tD(t=t1){return we((n,e)=>{let i=!1;n.subscribe(be(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function t1(){return new ca}function ua(t){return we((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Fr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Ie((r,o)=>t(r,o,i)):$n,lt(1),e?eD(n):tD(()=>new ca))}function Lu(t){return t<=0?()=>ft:we((n,e)=>{let i=[];n.subscribe(be(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Vu(){return we((t,n)=>{let e,i=!1;t.subscribe(be(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Gl(t={}){let{connector:n=()=>new k,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,u=!1,m=!1,v=()=>{s?.unsubscribe(),s=void 0},_=()=>{v(),a=l=void 0,u=m=!1},b=()=>{let A=a;_(),A?.unsubscribe()};return we((A,R)=>{c++,!m&&!u&&v();let L=l=l??n();R.add(()=>{c--,c===0&&!m&&!u&&(s=eg(b,r))}),L.subscribe(R),!a&&c>0&&(a=new Pr({next:_e=>L.next(_e),error:_e=>{m=!0,v(),s=eg(_,e,_e),L.error(_e)},complete:()=>{u=!0,v(),s=eg(_,i),L.complete()}}),et(A).subscribe(a))})(o)}}function eg(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Pr({next:()=>{i.unsubscribe(),t()}});return et(n(...e)).subscribe(i)}function ju(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Gl({connector:()=>new po(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Wl(t){return Ie((n,e)=>t<=e)}function qe(...t){let n=ar(t);return we((e,i)=>{(n?_o(t,e,n):_o(t,e)).subscribe(i)})}function mt(t,n){return we((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(be(i,l=>{r?.unsubscribe();let c=0,u=o++;et(t(l,u)).subscribe(r=be(i,m=>i.next(n?n(l,m,u,c++):m),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function Te(t){return we((n,e)=>{et(t).subscribe(be(e,()=>e.complete(),Fl)),!e.closed&&n.subscribe(e)})}function tg(t,n=!1){return we((e,i)=>{let r=0;e.subscribe(be(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function jt(t,n,e){let i=Ee(t)||n||e?{next:t,error:n,complete:e}:t;return i?we((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(be(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):$n}var ng;function Bu(){return ng}function lr(t){let n=ng;return ng=t,n}var nD=Symbol("NotFound");function cs(t){return t===nD||t?.name==="\u0275NotFound"}function ig(t,n,e){let i=Object.create(n1);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(na(i),uo(i),i.value===or)throw i.error;return i.value};return o[bt]=i,kl(i),o}function iD(t,n){na(t),ia(t,n),Ja(t)}function rD(t,n){if(na(t),t.value===or)throw t.error;au(t,n),Ja(t)}var n1=fe(D({},co),{value:ea,dirty:!0,error:null,equal:Ol,kind:"linkedSignal",producerMustRecompute(t){return t.value===ea||t.value===ta},producerRecomputeValue(t){if(t.value===ta)throw new Error("");let n=t.value;t.value=ta;let e=Nr(t),i,r=!1;try{let o=t.source(),a=n!==ea&&n!==or,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,ce(null),r=a&&i!==or&&t.equal(n,i)}catch(o){i=or,t.error=o}finally{fo(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function oD(t){let n=ce(null);try{return t()}finally{ce(n)}}var qu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",P=class extends Error{code;constructor(n,e){super(cr(n,e)),this.code=n}};function i1(t){return`NG0${Math.abs(t)}`}function cr(t,n){return`${i1(t)}${n?": "+n:""}`}var ai=globalThis;function Ge(t){for(let n in t)if(t[n]===Ge)return n;throw Error("")}function dD(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Jl(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Jl).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Yu(t,n){return t?n?`${t} ${n}`:t:n||""}var r1=Ge({__forward_ref__:Ge});function dn(t){return t.__forward_ref__=dn,t}function Nt(t){return gg(t)?t():t}function gg(t){return typeof t=="function"&&t.hasOwnProperty(r1)&&t.__forward_ref__===dn}function w(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function B(t){return{providers:t.providers||[],imports:t.imports||[]}}function ec(t){return o1(t,Qu)}function vg(t){return ec(t)!==null}function o1(t,n){return t.hasOwnProperty(n)&&t[n]||null}function a1(t){let n=t?.[Qu]??null;return n||null}function og(t){return t&&t.hasOwnProperty(Uu)?t[Uu]:null}var Qu=Ge({\u0275prov:Ge}),Uu=Ge({\u0275inj:Ge}),C=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=w({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function _g(t){return t&&!!t.\u0275providers}var yg=Ge({\u0275cmp:Ge}),bg=Ge({\u0275dir:Ge}),Cg=Ge({\u0275pipe:Ge}),Dg=Ge({\u0275mod:Ge}),Yl=Ge({\u0275fac:Ge}),ga=Ge({__NG_ELEMENT_ID__:Ge}),aD=Ge({__NG_ENV_ID__:Ge});function wg(t){return Zu(t,"@NgModule"),t[Dg]||null}function ki(t){return Zu(t,"@Component"),t[yg]||null}function tc(t){return Zu(t,"@Directive"),t[bg]||null}function xg(t){return Zu(t,"@Pipe"),t[Cg]||null}function Zu(t,n){if(t==null)throw new P(-919,!1)}function dr(t){return typeof t=="string"?t:t==null?"":String(t)}var uD=Ge({ngErrorCode:Ge}),s1=Ge({ngErrorMessage:Ge}),l1=Ge({ngTokenPath:Ge});function Eg(t,n){return fD("",-200,n)}function Ku(t,n){throw new P(-201,!1)}function fD(t,n,e){let i=new P(n,t);return i[uD]=n,i[s1]=t,e&&(i[l1]=e),i}function c1(t){return t[uD]}var ag;function mD(){return ag}function ln(t){let n=ag;return ag=t,n}function Sg(t,n,e){let i=ec(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Ku(t,"")}var d1={},fa=d1,u1="__NG_DI_FLAG__",sg=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=ma(e)||0;try{return this.injector.get(n,i&8?null:fa,i)}catch(r){if(cs(r))return r;throw r}}};function f1(t,n=0){let e=Bu();if(e===void 0)throw new P(-203,!1);if(e===null)return Sg(t,void 0,n);{let i=m1(n),r=e.retrieve(t,i);if(cs(r)){if(i.optional)return null;throw r}return r}}function le(t,n=0){return(mD()||f1)(Nt(t),n)}function d(t,n){return le(t,ma(n))}function ma(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function m1(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function lg(t){let n=[];for(let e=0;e<t.length;e++){let i=Nt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new P(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=p1(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(le(r,o))}else n.push(le(i))}return n}function p1(t){return t[u1]}function bo(t,n){let e=t.hasOwnProperty(Yl);return e?t[Yl]:null}function pD(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function hD(t){return t.flat(Number.POSITIVE_INFINITY)}function Xu(t,n){t.forEach(e=>Array.isArray(e)?Xu(e,n):n(e))}function Mg(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function nc(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function gD(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function vD(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Ju(t,n,e){let i=us(t,n);return i>=0?t[i|1]=e:(i=~i,vD(t,i,n,e)),i}function ef(t,n){let e=us(t,n);if(e>=0)return t[e|1]}function us(t,n){return h1(t,n,1)}function h1(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Oi={},Xt=[],ur=new C(""),Ig=new C("",-1),Tg=new C(""),Ql=class{get(n,e=fa){if(e===fa){let r=fD("",-201);throw r.name="\u0275NotFound",r}return e}};function fr(t){return{\u0275providers:t}}function _D(t){return fr([{provide:ur,multi:!0,useValue:t}])}function yD(...t){return{\u0275providers:tf(!0,t),\u0275fromNgModule:!0}}function tf(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return Xu(n,a=>{let s=a;zu(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&bD(r,o),e}function bD(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Ag(r,o=>{n(o,i)})}}function zu(t,n,e,i){if(t=Nt(t),!t)return!1;let r=null,o=og(t),a=!o&&ki(t);if(!o&&!a){let l=t.ngModule;if(o=og(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)zu(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;Xu(o.imports,u=>{zu(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&bD(c,n)}if(!s){let c=bo(r)||(()=>new r);n({provide:r,useFactory:c,deps:Xt},r),n({provide:Tg,useValue:r,multi:!0},r),n({provide:ur,useValue:()=>le(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;Ag(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Ag(t,n){for(let e of t)_g(e)&&(e=e.\u0275providers),Array.isArray(e)?Ag(e,n):n(e)}var g1=Ge({provide:String,useValue:Ge});function CD(t){return t!==null&&typeof t=="object"&&g1 in t}function v1(t){return!!(t&&t.useExisting)}function _1(t){return!!(t&&t.useFactory)}function pa(t){return typeof t=="function"}function DD(t){return!!t.useClass}var ic=new C(""),Hu={},sD={},rg;function fs(){return rg===void 0&&(rg=new Ql),rg}var We=class{},ha=class extends We{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,dg(n,a=>this.processProvider(a)),this.records.set(Ig,ds(void 0,this)),r.has("environment")&&this.records.set(We,ds(void 0,this));let o=this.records.get(ic);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Tg,Xt,{self:!0}))}retrieve(n,e){let i=ma(e)||0;try{return this.get(n,fa,i)}catch(r){if(cs(r))return r;throw r}}destroy(){ql(this),this._destroyed=!0;let n=ce(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ce(n)}}onDestroy(n){return ql(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ql(this);let e=lr(this),i=ln(void 0),r;try{return n()}finally{lr(e),ln(i)}}get(n,e=fa,i){if(ql(this),n.hasOwnProperty(aD))return n[aD](this);let r=ma(i),o,a=lr(this),s=ln(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=w1(n)&&ec(n);u&&this.injectableDefInScope(u)?c=ds(cg(n),Hu):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?fs():this.parent;return e=r&8&&e===fa?null:e,l.get(n,e)}catch(l){let c=c1(l);throw c===-200||c===-201?new P(c,null):l}finally{ln(s),lr(a)}}resolveInjectorInitializers(){let n=ce(null),e=lr(this),i=ln(void 0),r;try{let o=this.get(ur,Xt,{self:!0});for(let a of o)a()}finally{lr(e),ln(i),ce(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Nt(n);let e=pa(n)?n:Nt(n&&n.provide),i=b1(n);if(!pa(n)&&n.multi===!0){let r=this.records.get(e);r||(r=ds(void 0,Hu,!0),r.factory=()=>lg(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ce(null);try{if(e.value===sD)throw Eg("");return e.value===Hu&&(e.value=sD,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&D1(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ce(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Nt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function cg(t){let n=ec(t),e=n!==null?n.factory:bo(t);if(e!==null)return e;if(t instanceof C)throw new P(-204,!1);if(t instanceof Function)return y1(t);throw new P(-204,!1)}function y1(t){if(t.length>0)throw new P(-204,!1);let e=a1(t);return e!==null?()=>e.factory(t):()=>new t}function b1(t){if(CD(t))return ds(void 0,t.useValue);{let n=Rg(t);return ds(n,Hu)}}function Rg(t,n,e){let i;if(pa(t)){let r=Nt(t);return bo(r)||cg(r)}else if(CD(t))i=()=>Nt(t.useValue);else if(_1(t))i=()=>t.useFactory(...lg(t.deps||[]));else if(v1(t))i=(r,o)=>le(Nt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Nt(t&&(t.useClass||t.provide));if(C1(t))i=()=>new r(...lg(t.deps));else return bo(r)||cg(r)}return i}function ql(t){if(t.destroyed)throw new P(-205,!1)}function ds(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function C1(t){return!!t.deps}function D1(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function w1(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function dg(t,n){for(let e of t)Array.isArray(e)?dg(e,n):e&&_g(e)?dg(e.\u0275providers,n):n(e)}function Bt(t,n){let e;t instanceof ha?(ql(t),e=t):e=new sg(t);let i,r=lr(e),o=ln(void 0);try{return n()}finally{lr(r),ln(o)}}function wD(){return mD()!==void 0||Bu()!=null}var un=0,ne=1,he=2,Dt=3,si=4,fn=5,mn=6,ms=7,wt=8,Gn=9,Ni=10,Ve=11,ps=12,kg=13,va=14,zt=15,wo=16,_a=17,mr=18,Vr=19,Og=20,Lr=21,nf=22,Co=23,Wn=24,ya=25,xo=26,Ne=27,xD=1,Pi=6,pr=7,rc=8,ba=9,pt=10;function li(t){return Array.isArray(t)&&typeof t[xD]=="object"}function qn(t){return Array.isArray(t)&&t[xD]===!0}function Ng(t){return(t.flags&4)!==0}function hr(t){return t.componentOffset>-1}function hs(t){return(t.flags&1)===1}function Fi(t){return!!t.template}function Ca(t){return(t[he]&512)!==0}function Eo(t){return(t[he]&256)===256}var Pg="svg",ED="math";function Yn(t){for(;Array.isArray(t);)t=t[un];return t}function Fg(t,n){return Yn(n[t])}function ci(t,n){return Yn(n[t.index])}function gs(t,n){return t.data[n]}function rf(t,n){return t[n]}function Lg(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function di(t,n){let e=n[t];return li(e)?e:e[un]}function SD(t){return(t[he]&4)===4}function of(t){return(t[he]&128)===128}function MD(t){return qn(t[Dt])}function Qn(t,n){return n==null?null:t[n]}function Vg(t){t[_a]=0}function jg(t){t[he]&1024||(t[he]|=1024,of(t)&&Da(t))}function ID(t,n){for(;t>0;)n=n[va],t--;return n}function oc(t){return!!(t[he]&9216||t[Wn]?.dirty)}function af(t){t[Ni].changeDetectionScheduler?.notify(8),t[he]&64&&(t[he]|=1024),oc(t)&&Da(t)}function Da(t){t[Ni].changeDetectionScheduler?.notify(0);let n=Do(t);for(;n!==null&&!(n[he]&8192||(n[he]|=8192,!of(n)));)n=Do(n)}function Bg(t,n){if(Eo(t))throw new P(911,!1);t[Lr]===null&&(t[Lr]=[]),t[Lr].push(n)}function TD(t,n){if(t[Lr]===null)return;let e=t[Lr].indexOf(n);e!==-1&&t[Lr].splice(e,1)}function Do(t){let n=t[Dt];return qn(n)?n[Dt]:n}function Hg(t){return t[ms]??=[]}function Ug(t){return t.cleanup??=[]}function AD(t,n,e,i){let r=Hg(n);r.push(e),t.firstCreatePass&&Ug(t).push(i,r.length-1)}var Se={lFrame:UD(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var ug=!1;function RD(){return Se.lFrame.elementDepthCount}function kD(){Se.lFrame.elementDepthCount++}function zg(){Se.lFrame.elementDepthCount--}function sf(){return Se.bindingsEnabled}function lf(){return Se.skipHydrationRootTNode!==null}function $g(t){return Se.skipHydrationRootTNode===t}function OD(t){Se.skipHydrationRootTNode=t}function Gg(){Se.skipHydrationRootTNode=null}function se(){return Se.lFrame.lView}function nt(){return Se.lFrame.tView}function Ae(t){return Se.lFrame.contextLView=t,t[wt]}function Re(t){return Se.lFrame.contextLView=null,t}function Pt(){let t=Wg();for(;t!==null&&t.type===64;)t=t.parent;return t}function Wg(){return Se.lFrame.currentTNode}function ND(){let t=Se.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function vs(t,n){let e=Se.lFrame;e.currentTNode=t,e.isParent=n}function qg(){return Se.lFrame.isParent}function Yg(){Se.lFrame.isParent=!1}function PD(){return Se.lFrame.contextLView}function Qg(){return ug}function Zl(t){let n=ug;return ug=t,n}function _s(){let t=Se.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Zg(){return Se.lFrame.bindingIndex}function FD(t){return Se.lFrame.bindingIndex=t}function jr(){return Se.lFrame.bindingIndex++}function ac(t){let n=Se.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function LD(){return Se.lFrame.inI18n}function VD(t,n){let e=Se.lFrame;e.bindingIndex=e.bindingRootIndex=t,cf(n)}function jD(){return Se.lFrame.currentDirectiveIndex}function cf(t){Se.lFrame.currentDirectiveIndex=t}function BD(t){let n=Se.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function df(){return Se.lFrame.currentQueryIndex}function sc(t){Se.lFrame.currentQueryIndex=t}function x1(t){let n=t[ne];return n.type===2?n.declTNode:n.type===1?t[fn]:null}function Kg(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=x1(o),r===null||(o=o[va],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=Se.lFrame=HD();return i.currentTNode=n,i.lView=t,!0}function uf(t){let n=HD(),e=t[ne];Se.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function HD(){let t=Se.lFrame,n=t===null?null:t.child;return n===null?UD(t):n}function UD(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function zD(){let t=Se.lFrame;return Se.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Xg=zD;function ff(){let t=zD();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function $D(t){return(Se.lFrame.contextLView=ID(t,Se.lFrame.contextLView))[wt]}function Li(){return Se.lFrame.selectedIndex}function So(t){Se.lFrame.selectedIndex=t}function lc(){let t=Se.lFrame;return gs(t.tView,t.selectedIndex)}function gr(){Se.lFrame.currentNamespace=Pg}function mf(){return Se.lFrame.currentNamespace}var GD=!0;function pf(){return GD}function Br(t){GD=t}function fg(t,n=null,e=null,i){let r=Jg(t,n,e,i);return r.resolveInjectorInitializers(),r}function Jg(t,n=null,e=null,i,r=new Set){let o=[e||Xt,yD(t)],a;return new ha(o,n||fs(),a||null,r)}var oe=class t{static THROW_IF_NOT_FOUND=fa;static NULL=new Ql;static create(n,e){if(Array.isArray(n))return fg({name:""},e,n,"");{let i=n.name??"";return fg({name:i},n.parent,n.providers,i)}}static \u0275prov=w({token:t,providedIn:"any",factory:()=>le(Ig)});static __NG_ELEMENT_ID__=-1},V=new C(""),pn=(()=>{class t{static __NG_ELEMENT_ID__=E1;static __NG_ENV_ID__=e=>e}return t})(),$u=class extends pn{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Eo(this._lView)}onDestroy(n){let e=this._lView;return Bg(e,n),()=>TD(e,n)}};function E1(){return new $u(se())}var WD=!1,qD=new C(""),vr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Ct(!1);debugTaskTracker=d(qD,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ye(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),mg=class extends k{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,wD()&&(this.destroyRef=d(pn,{optional:!0})??void 0,this.pendingTasks=d(vr,{optional:!0})??void 0)}emit(n){let e=ce(null);try{super.next(n)}finally{ce(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof me&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Q=mg;function Gu(...t){}function ev(t){let n,e;function i(){t=Gu;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function YD(t){return queueMicrotask(()=>t()),()=>{t=Gu}}var tv="isAngularZone",Kl=tv+"_ID",S1=0,ie=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Q(!1);onMicrotaskEmpty=new Q(!1);onStable=new Q(!1);onError=new Q(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=WD}=n;if(typeof Zone>"u")throw new P(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,T1(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(tv)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new P(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new P(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,M1,Gu,Gu);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},M1={};function nv(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function I1(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){ev(()=>{t.callbackScheduled=!1,pg(t),t.isCheckStableRunning=!0,nv(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),pg(t)}function T1(t){let n=()=>{I1(t)},e=S1++;t._inner=t._inner.fork({name:"angular",properties:{[tv]:!0,[Kl]:e,[Kl+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(A1(l))return i.invokeTask(o,a,s,l);try{return lD(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),cD(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return lD(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!R1(l)&&n(),cD(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,pg(t),nv(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function pg(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function lD(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function cD(t){t._nesting--,nv(t)}var Xl=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Q;onMicrotaskEmpty=new Q;onStable=new Q;onError=new Q;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function A1(t){return QD(t,"__ignore_ng_zone__")}function R1(t){return QD(t,"__scheduler_tick__")}function QD(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var cn=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Zn=new C("",{factory:()=>{let t=d(ie),n=d(We),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(cn),e.handleError(i))})}}}),ZD={provide:ur,useValue:()=>{let t=d(cn,{optional:!0})},multi:!0},k1=new C("",{factory:()=>{let t=d(V).defaultView;if(!t)return;let n=d(Zn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(pn).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function iv(){return fr([_D(()=>{d(k1)})])}function S(t,n){let[e,i,r]=jh(t,n?.equal),o=e,a=o[bt];return o.set=i,o.update=r,o.asReadonly=hf.bind(o),o}function hf(){let t=this[bt];if(t.readonlyFn===void 0){let n=()=>this();n[bt]=t,t.readonlyFn=n}return t.readonlyFn}var ys=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=O1}return t})();function O1(){return new ys(se(),Pt())}var Ri=class{},cc=new C("",{factory:()=>!0});var rv=new C(""),bs=(()=>{class t{internalPendingTasks=d(vr);scheduler=d(Ri);errorHandler=d(Zn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),gf=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>new hg})}return t})(),hg=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Wu=class{[bt];constructor(n){this[bt]=n}destroy(){this[bt].destroy()}};function Vi(t,n){let e=n?.injector??d(oe),i=n?.manualCleanup!==!0?e.get(pn):null,r,o=e.get(ys,null,{optional:!0}),a=e.get(Ri);return o!==null?(r=F1(o.view,a,t),i instanceof $u&&i._lView===o.view&&(i=null)):r=L1(t,e.get(gf),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Wu(r)}var KD=fe(D({},Bh),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Zl(!1);try{Hh(this)}finally{Zl(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ce(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ce(t)}}}),N1=fe(D({},KD),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(mo(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),P1=fe(D({},KD),{consumerMarkedDirty(){this.view[he]|=8192,Da(this.view),this.notifier.notify(13)},destroy(){if(mo(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Co]?.delete(this)}});function F1(t,n,e){let i=Object.create(P1);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=XD(i,e),t[Co]??=new Set,t[Co].add(i),i.consumerMarkedDirty(i),i}function L1(t,n,e){let i=Object.create(N1);return i.fn=XD(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function XD(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}var vf={JSACTION:"jsaction"};function Dc(t){return{toString:t}.toString()}function G1(t){return typeof t=="function"}function $w(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Tf=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Pe=(()=>{let t=()=>Gw;return t.ngInherit=!0,t})();function Gw(t){return t.type.prototype.ngOnChanges&&(t.setInput=q1),W1}function W1(){let t=qw(this),n=t?.current;if(n){let e=t.previous;if(e===Oi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function q1(t,n,e,i,r){let o=this.declaredInputs[i],a=qw(t)||Y1(t,{previous:Oi,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Tf(c&&c.currentValue,e,l===Oi),$w(t,n,r,e)}var Ww="__ngSimpleChanges__";function qw(t){return t[Ww]||null}function Y1(t,n){return t[Ww]=n}var JD=[];var Be=function(t,n=null,e){for(let i=0;i<JD.length;i++){let r=JD[i];r(t,n,e)}},Oe=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Oe||{});function Q1(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=Gw(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Yw(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Df(t,n,e){Qw(t,n,3,e)}function wf(t,n,e,i){(t[he]&3)===e&&Qw(t,n,e,i)}function ov(t,n){let e=t[he];(e&3)===n&&(e&=16383,e+=1,t[he]=e)}function Qw(t,n,e,i){let r=i!==void 0?t[_a]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[_a]+=65536),(s<o||o==-1)&&(Z1(t,e,n,l),t[_a]=(t[_a]&4294901760)+l+2),l++}function ew(t,n){Be(Oe.LifecycleHookStart,t,n);let e=ce(null);try{n.call(t)}finally{ce(e),Be(Oe.LifecycleHookEnd,t,n)}}function Z1(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[he]>>14<t[_a]>>16&&(t[he]&3)===n&&(t[he]+=16384,ew(s,o)):ew(s,o)}var Ds=-1,Ea=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function K1(t){return(t.flags&8)!==0}function X1(t){return(t.flags&16)!==0}function J1(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];eO(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function Zw(t){return t===3||t===4||t===6}function eO(t){return t.charCodeAt(0)===64}function Es(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?tw(t,e,r,null,n[++i]):tw(t,e,r,null,null))}}return t}function tw(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Kw(t){return t!==Ds}function Af(t){return t&32767}function tO(t){return t>>16}function Rf(t,n){let e=tO(t),i=n;for(;e>0;)i=i[va],e--;return i}var yv=!0;function kf(t){let n=yv;return yv=t,n}var nO=256,Xw=nO-1,Jw=5,iO=0,_r={};function rO(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(ga)&&(i=e[ga]),i==null&&(i=e[ga]=iO++);let r=i&Xw,o=1<<r;n.data[t+(r>>Jw)]|=o}function Of(t,n){let e=ex(t,n);if(e!==-1)return e;let i=n[ne];i.firstCreatePass&&(t.injectorIndex=n.length,av(i.data,t),av(n,null),av(i.blueprint,null));let r=o_(t,n),o=t.injectorIndex;if(Kw(r)){let a=Af(r),s=Rf(r,n),l=s[ne].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function av(t,n){t.push(0,0,0,0,0,0,0,0,n)}function ex(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function o_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=ox(r),i===null)return Ds;if(e++,r=r[va],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ds}function bv(t,n,e){rO(t,n,e)}function oO(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Zw(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function tx(t,n,e){if(e&8||t!==void 0)return t;Ku(n,"NodeInjector")}function nx(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Gn],o=ln(void 0);try{return r?r.get(n,i,e&8):Sg(n,i,e&8)}finally{ln(o)}}return tx(i,n,e)}function ix(t,n,e,i=0,r){if(t!==null){if(n[he]&2048&&!(i&2)){let a=cO(t,n,e,i,_r);if(a!==_r)return a}let o=rx(t,n,e,i,_r);if(o!==_r)return o}return nx(n,e,i,r)}function rx(t,n,e,i,r){let o=sO(e);if(typeof o=="function"){if(!Kg(n,t,i))return i&1?tx(r,e,i):nx(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))Ku(e);else return a}finally{Xg()}}else if(typeof o=="number"){let a=null,s=ex(t,n),l=Ds,c=i&1?n[zt][fn]:null;for((s===-1||i&4)&&(l=s===-1?o_(t,n):n[s+8],l===Ds||!iw(i,!1)?s=-1:(a=n[ne],s=Af(l),n=Rf(l,n)));s!==-1;){let u=n[ne];if(nw(o,s,u.data)){let m=aO(s,n,e,a,i,c);if(m!==_r)return m}l=n[s+8],l!==Ds&&iw(i,n[ne].data[s+8]===c)&&nw(o,s,n)?(a=u,s=Af(l),n=Rf(l,n)):s=-1}}return r}function aO(t,n,e,i,r,o){let a=n[ne],s=a.data[t+8],l=i==null?hr(s)&&yv:i!=a&&(s.type&3)!==0,c=r&1&&o===s,u=xf(s,a,e,l,c);return u!==null?pc(n,a,u,s,r):_r}function xf(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,m=i?s:s+u,v=r?s+u:c;for(let _=m;_<v;_++){let b=a[_];if(_<l&&e===b||_>=l&&b.type===e)return _}if(r){let _=a[l];if(_&&Fi(_)&&_.type===e)return l}return null}function pc(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof Ea){let s=o;if(s.resolving)throw Eg("");let l=kf(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],u,m=s.injectImpl?ln(s.injectImpl):null,v=Kg(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&Q1(e,a[e],n)}finally{m!==null&&ln(m),kf(l),s.resolving=!1,Xg()}}return o}function sO(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(ga)?t[ga]:void 0;return typeof n=="number"?n>=0?n&Xw:lO:n}function nw(t,n,e){let i=1<<t;return!!(e[n+(t>>Jw)]&i)}function iw(t,n){return!(t&2)&&!(t&1&&n)}var xa=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return ix(this._tNode,this._lView,n,ma(i),e)}};function lO(){return new xa(Pt(),se())}function Fe(t){return Dc(()=>{let n=t.prototype.constructor,e=n[Yl]||Cv(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Yl]||Cv(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Cv(t){return gg(t)?()=>{let n=Cv(Nt(t));return n&&n()}:bo(t)}function cO(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[he]&2048&&!Ca(a);){let s=rx(o,a,e,i|2,_r);if(s!==_r)return s;let l=o.parent;if(!l){let c=a[Og];if(c){let u=c.get(e,_r,i&-5);if(u!==_r)return u}l=ox(a),a=a[va]}o=l}return r}function ox(t){let n=t[ne],e=n.type;return e===2?n.declTNode:e===1?t[fn]:null}function wc(t){return oO(Pt(),t)}function dO(){return Ts(Pt(),se())}function Ts(t,n){return new G(ci(t,n))}var G=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=dO}return t})();function ax(t){return t instanceof G?t.nativeElement:t}function uO(){return this._results[Symbol.iterator]()}var Jt=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new k}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=hD(n);(this._changesDetected=!pD(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=uO},sx="ngSkipHydration",fO="ngskiphydration";function lx(t){let n=t.mergedAttrs;if(n===null)return!1;for(let e=0;e<n.length;e+=2){let i=n[e];if(typeof i=="number")return!1;if(typeof i=="string"&&i.toLowerCase()===fO)return!0}return!1}function cx(t){return t.hasAttribute(sx)}function Nf(t){return(t.flags&128)===128}function dx(t){if(Nf(t))return!0;let n=t.parent;for(;n;){if(Nf(t)||lx(n))return!0;n=n.parent}return!1}var a_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(a_||{}),ux=new Map,mO=0;function pO(){return mO++}function hO(t){ux.set(t[Vr],t)}function Dv(t){ux.delete(t[Vr])}var rw="__ngContext__";function Ss(t,n){li(n)?(t[rw]=n[Vr],hO(n)):t[rw]=n}function fx(t){return px(t[ps])}function mx(t){return px(t[si])}function px(t){for(;t!==null&&!qn(t);)t=t[si];return t}var wv;function s_(t){wv=t}function hx(){if(wv!==void 0)return wv;if(typeof document<"u")return document;throw new P(210,!1)}var ui=new C("",{factory:()=>gO}),gO="ng";var qf=new C(""),Ur=new C("",{providedIn:"platform",factory:()=>"unknown"}),xc=new C(""),Ta=new C("",{factory:()=>d(V).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var As=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>{let e=new t;return e.store=vO(d(V),d(ui)),e}});store={};onSerializeCallbacks={};get(e,i){return this.store[e]!==void 0?this.store[e]:i}set(e,i){this.store[e]=i}remove(e){delete this.store[e]}hasKey(e){return this.store.hasOwnProperty(e)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(e,i){this.onSerializeCallbacks[e]=i}toJson(){for(let e in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(e))try{this.store[e]=this.onSerializeCallbacks[e]()}catch(i){console.warn("Exception in onSerialize callback: ",i)}return JSON.stringify(this.store).replace(/</g,"\\u003C").replace(/\//g,"\\u002F")}}return t})();function vO(t,n){let e=t.getElementById(n+"-state");if(e?.textContent)try{return JSON.parse(e.textContent)}catch(i){console.warn("Exception while restoring TransferState for app "+n,i)}return{}}var gx="h",vx="b",_O="f",yO="n",_x="e",yx="t",Yf="c",l_="x",hc="r",bx="i",Cx="n",c_="d";var Dx="di",wx="s",xx="p";var Rs=new C(""),Ex=!1,d_=new C("",{factory:()=>Ex});var u_=new C(""),Sx=!1,Mx=new C("",{factory:()=>[]}),Ix=new C(""),f_=new C("",{factory:()=>new Map});var Ec="ngb";var Tx=(t,n,e)=>{let i=t,r=i.__jsaction_fns??new Map,o=r.get(n)??[];o.push(e),r.set(n,o),i.__jsaction_fns=r},Ax=(t,n)=>{let e=t,i=e.getAttribute(Ec)??"",r=n.get(i)??new Set;r.has(e)||r.add(e),n.set(i,r)};var Rx=t=>{t.removeAttribute(vf.JSACTION),t.removeAttribute(Ec),t.__jsaction_fns=void 0},kx=new C("",{factory:()=>({})}),xv=new WeakMap;function bO(t,n){if(t==null||typeof t!="object")return;let e=xv.get(t);e||(e=new WeakSet,xv.set(t,e)),e.add(n)}function m_(t,n){let e=n?.__jsaction_fns?.get(t.type);if(!(!e||!n?.isConnected)&&!(n&&xv.get(t)?.has(n)))for(let i of e)i(t)}var Ev=new Map;function Ox(t,n){return Ev.set(t,n),()=>Ev.delete(t)}var ow=!1,Nx=(t,n,e,i)=>{};function CO(t,n,e,i){Nx(t,n,e,i)}function Px(){ow||(Nx=(t,n,e,i)=>{let r=t[Gn].get(ui);Ev.get(r)?.(n,e,i)},ow=!0)}var Qf=new C("");function Sc(t){return(t.flags&32)===32}var DO="__nghData__",p_=DO,wO="__nghDeferData__",Fx=wO;var Ef="ngh",Lx="nghm",Vx=()=>null;function xO(t,n,e=!1){let i=t.getAttribute(Ef);if(i==null)return null;let[r,o]=i.split("|");if(i=e?o:r,!i)return null;let a=o?`|${o}`:"",s=e?r:a,l={};if(i!==""){let u=n.get(As,null,{optional:!0});u!==null&&(l=u.get(p_,[])[Number(i)])}let c={data:l,firstChild:t.firstChild??null};return e&&(c.firstChild=t,Zf(c,0,t.nextSibling)),s?t.setAttribute(Ef,s):t.removeAttribute(Ef),c}function jx(){Vx=xO}function Bx(t,n,e=!1){return Vx(t,n,e)}function Hx(t){let n=t._lView;return n[ne].type===2?null:(Ca(n)&&(n=n[Ne]),n)}function EO(t){return t.textContent?.replace(/\s/gm,"")}function SO(t){let n=hx(),e=n.createNodeIterator(t,NodeFilter.SHOW_COMMENT,{acceptNode(o){let a=EO(o);return a==="ngetn"||a==="ngtns"?NodeFilter.FILTER_ACCEPT:NodeFilter.FILTER_REJECT}}),i,r=[];for(;i=e.nextNode();)r.push(i);for(let o of r)o.textContent==="ngetn"?o.replaceWith(n.createTextNode("")):o.remove()}function Zf(t,n,e){t.segmentHeads??={},t.segmentHeads[n]=e}function Sv(t,n){return t.segmentHeads?.[n]??null}function Ux(t){return t.get(Ix,!1,{optional:!0})}function MO(t,n){let e=t.data,i=e[_x]?.[n]??null;return i===null&&e[Yf]?.[n]&&(i=h_(t,n)),i}function zx(t,n){return t.data[Yf]?.[n]??null}function h_(t,n){let e=zx(t,n)??[],i=0;for(let r of e)i+=r[hc]*(r[l_]??1);return i}function IO(t){if(typeof t.disconnectedNodes>"u"){let n=t.data[c_];t.disconnectedNodes=n?new Set(n):null}return t.disconnectedNodes}function $x(t,n){if(typeof t.disconnectedNodes>"u"){let e=t.data[c_];t.disconnectedNodes=e?new Set(e):null}return!!IO(t)?.has(n)}function Kf(t,n){let e=t[mn];return e!==null&&!lf()&&!Sc(n)&&!$x(e,n.index-Ne)}function TO(t,n){let e=n.get(Qf),r=n.get(As).get(Fx,{}),o=!1,a=t,s=null,l=[];for(;!o&&a;){o=e.has(a);let c=e.hydrating.get(a);if(s===null&&c!=null){s=c.promise;break}l.unshift(a),a=r[a][xx]}return{parentBlockPromise:s,hydrationQueue:l}}function sv(t){return!!t&&t.nodeType===Node.COMMENT_NODE&&t.textContent?.trim()===Lx}function aw(t){for(;t&&t.nodeType===Node.TEXT_NODE;)t=t.previousSibling;return t}function Gx(t){for(let i of t.body.childNodes)if(sv(i))return;let n=aw(t.body.previousSibling);if(sv(n))return;let e=aw(t.head.lastChild);if(!sv(e))throw new P(-507,!1)}function Wx(t,n){let e=t.contentQueries;if(e!==null){let i=ce(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];sc(o),s.contentQueries(2,n[a],a)}}}finally{ce(i)}}}function Mv(t,n,e){sc(0);let i=ce(null);try{n(t,e)}finally{ce(i)}}function g_(t,n,e){if(Ng(n)){let i=ce(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{ce(i)}}}var Hi=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Hi||{});var _f;function AO(){if(_f===void 0&&(_f=null,ai.trustedTypes))try{_f=ai.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return _f}function Xf(t){return AO()?.createHTML(t)||t}var yf;function RO(){if(yf===void 0&&(yf=null,ai.trustedTypes))try{yf=ai.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return yf}function sw(t){return RO()?.createScriptURL(t)||t}var Hr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${qu})`}},Iv=class extends Hr{getTypeName(){return"HTML"}},Tv=class extends Hr{getTypeName(){return"Style"}},Av=class extends Hr{getTypeName(){return"Script"}},Rv=class extends Hr{getTypeName(){return"URL"}},kv=class extends Hr{getTypeName(){return"ResourceURL"}};function Ui(t){return t instanceof Hr?t.changingThisBreaksApplicationSecurity:t}function zr(t,n){let e=qx(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${qu})`)}return e===n}function qx(t){return t instanceof Hr&&t.getTypeName()||null}function v_(t){return new Iv(t)}function __(t){return new Tv(t)}function y_(t){return new Av(t)}function b_(t){return new Rv(t)}function C_(t){return new kv(t)}function kO(t){let n=new Nv(t);return OO()?new Ov(n):n}var Ov=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Xf(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Nv=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Xf(n),e}};function OO(){try{return!!new window.DOMParser().parseFromString(Xf(""),"text/html")}catch{return!1}}var NO=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Mc(t){return t=String(t),t.match(NO)?t:"unsafe:"+t}function $r(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Ic(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var Yx=$r("area,br,col,hr,img,wbr"),Qx=$r("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Zx=$r("rp,rt"),PO=Ic(Zx,Qx),FO=Ic(Qx,$r("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),LO=Ic(Zx,$r("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),lw=Ic(Yx,FO,LO,PO),Kx=$r("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),VO=$r("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),jO=$r("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),BO=Ic(Kx,VO,jO),HO=$r("script,style,template"),Pv=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=$O(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=zO(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=cw(n).toLowerCase();if(!lw.hasOwnProperty(e))return this.sanitizedSomething=!0,!HO.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!BO.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;Kx[s]&&(l=Mc(l)),this.buf.push(" ",a,'="',dw(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=cw(n).toLowerCase();lw.hasOwnProperty(e)&&!Yx.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(dw(n))}};function UO(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function zO(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Xx(n);return n}function $O(t){let n=t.firstChild;if(n&&UO(t,n))throw Xx(n);return n}function cw(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Xx(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var GO=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,WO=/([^\#-~ |!])/g;function dw(t){return t.replace(/&/g,"&amp;").replace(GO,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(WO,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var bf;function D_(t,n){let e=null;try{bf=bf||kO(t);let i=n?String(n):"";e=bf.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=bf.getInertBodyElement(i)}while(i!==o);let s=new Pv().sanitizeChildren(uw(e)||e);return Xf(s)}finally{if(e){let i=uw(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function uw(t){return"content"in t&&qO(t)?t.content:null}function qO(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var YO=/^>|^->|<!--|-->|--!>|<!-$/g,QO=/(<|>)/g,ZO="\u200B$1\u200B";function KO(t){return t.replace(YO,n=>n.replace(QO,ZO))}function Jx(t,n){return t.createText(n)}function XO(t,n,e){t.setValue(n,e)}function eE(t,n){return t.createComment(KO(n))}function w_(t,n,e){return t.createElement(n,e)}function Pf(t,n,e,i,r){t.insertBefore(n,e,i,r)}function tE(t,n,e){t.appendChild(n,e)}function fw(t,n,e,i,r){i!==null?Pf(t,n,e,i,r):tE(t,n,e)}function x_(t,n,e,i){t.removeChild(null,n,e,i)}function nE(t){t.textContent=""}function JO(t,n,e){t.setAttribute(n,"style",e)}function eN(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function iE(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&J1(t,n,i),r!==null&&eN(t,n,r),o!==null&&JO(t,n,o)}var Ft=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Ft||{});function rE(t){let n=aE();return n?n.sanitize(Ft.URL,t)||"":zr(t,"URL")?Ui(t):Mc(dr(t))}function oE(t){let n=aE();if(n)return sw(n.sanitize(Ft.RESOURCE_URL,t)||"");if(zr(t,"ResourceURL"))return sw(Ui(t));throw new P(904,!1)}var tN={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function nN(t,n){return tN[t.toLowerCase()]?.[n.toLowerCase()]===!0?oE:rE}function E_(t,n,e){return nN(n,e)(t)}function aE(){let t=se();return t&&t[Ni].sanitizer}function sE(t){return t.ownerDocument.body}function lE(t){return t instanceof Function?t():t}function iN(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var cE="ng-template";function rN(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&iN(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(S_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function S_(t){return t.type===4&&t.value!==cE}function oN(t,n,e){let i=t.type===4&&!e?cE:t.value;return n===i}function aN(t,n,e){let i=4,r=t.attrs,o=r!==null?cN(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!ji(i)&&!ji(l))return!1;if(a&&ji(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!oN(t,l,e)||l===""&&n.length===1){if(ji(i))return!1;a=!0}}else if(i&8){if(r===null||!rN(t,r,l,e)){if(ji(i))return!1;a=!0}}else{let c=n[++s],u=sN(l,r,S_(t),e);if(u===-1){if(ji(i))return!1;a=!0;continue}if(c!==""){let m;if(u>o?m="":m=r[u+1].toLowerCase(),i&2&&c!==m){if(ji(i))return!1;a=!0}}}}return ji(i)||a}function ji(t){return(t&1)===0}function sN(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return dN(n,t)}function dE(t,n,e=!1){for(let i=0;i<n.length;i++)if(aN(t,n[i],e))return!0;return!1}function lN(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function cN(t){for(let n=0;n<t.length;n++){let e=t[n];if(Zw(e))return n}return t.length}function dN(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function uN(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function mw(t,n){return t?":not("+n.trim()+")":n}function fN(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!ji(a)&&(n+=mw(o,r),r=""),i=a,o=o||!ji(i);e++}return r!==""&&(n+=mw(o,r)),n}function mN(t){return t.map(fN).join(",")}function pN(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!ji(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Ht={};function M_(t,n,e,i,r,o,a,s,l,c,u){let m=Ne+i,v=m+r,_=hN(m,v),b=typeof c=="function"?c():c;return _[ne]={type:t,blueprint:_,template:e,queries:null,viewQuery:s,declTNode:n,data:_.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:v,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:b,incompleteFirstPass:!1,ssrId:u}}function hN(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Ht);return e}function gN(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=M_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function I_(t,n,e,i,r,o,a,s,l,c,u){let m=n.blueprint.slice();return m[un]=r,m[he]=i|4|128|8|64|1024,(c!==null||t&&t[he]&2048)&&(m[he]|=2048),Vg(m),m[Dt]=m[va]=t,m[wt]=e,m[Ni]=a||t&&t[Ni],m[Ve]=s||t&&t[Ve],m[Gn]=l||t&&t[Gn]||null,m[fn]=o,m[Vr]=pO(),m[mn]=u,m[Og]=c,m[zt]=n.type==2?t[zt]:m,m}function vN(t,n,e){let i=ci(n,t),r=gN(e),o=t[Ni].rendererFactory,a=T_(t,I_(t,r,null,uE(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function uE(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function fE(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function T_(t,n){return t[ps]?t[kg][si]=n:t[ps]=n,t[kg]=n,n}function h(t=1){mE(nt(),se(),Li()+t,!1)}function mE(t,n,e,i){if(!i)if((n[he]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Df(n,o,e)}else{let o=t.preOrderHooks;o!==null&&wf(n,o,0,e)}So(e)}var Jf=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Jf||{});function Fv(t,n,e,i){let r=ce(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Jf.SignalBased)!==0&&(l=n[o][bt]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):$w(n,l,o,i)}finally{ce(r)}}var yr=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(yr||{}),_N;function A_(t,n){return _N(t,n)}var yK=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Lv=new WeakMap,uc=new WeakSet;function yN(t,n){let e=Lv.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),uc.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function bN(t,n){let e=Lv.get(t);e?e.includes(n)||e.push(n):Lv.set(t,[n])}var Sa=new Set,em=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(em||{}),zi=new C(""),pw=new Set;function fi(t){pw.has(t)||(pw.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var tm=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),R_=[0,1,2,3],k_=(()=>{class t{ngZone=d(ie);scheduler=d(Ri);errorHandler=d(cn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(zi,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Be(Oe.AfterRenderHooksStart),this.executing=!0;for(let i of R_)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Be(Oe.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[ya]??=[]).push(e),Da(i),i[he]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(em.AFTER_NEXT_RENDER,e):e()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),gc=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[ya];n&&(this.view[ya]=n.filter(e=>e!==this))}};function ht(t,n){let e=n?.injector??d(oe);return fi("NgAfterNextRender"),DN(t,e,n,!0)}function CN(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function DN(t,n,e,i){let r=n.get(tm);r.impl??=n.get(k_);let o=n.get(zi,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(pn):null,s=n.get(ys,null,{optional:!0}),l=new gc(r.impl,CN(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var pE=new C("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(We)})});function hE(t,n,e){let i=t.get(pE);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function wN(t,n){let e=t.get(pE);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function xN(t,n){for(let[e,i]of n)hE(t,i.animateFns)}function hw(t,n,e,i){let r=t?.[xo]?.enter;n!==null&&r&&r.has(e.index)&&xN(i,r)}function Cs(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;qn(r)?l=r:li(r)&&(c=!0,r=r[un]);let u=Yn(r);t===0&&i!==null?(hw(s,i,o,e),a==null?tE(n,i,u):Pf(n,i,u,a||null,!0)):t===1&&i!==null?(hw(s,i,o,e),Pf(n,i,u,a||null,!0),yN(o,u)):t===2?(s?.[xo]?.leave?.has(o.index)&&bN(o,u),uc.delete(u),gw(s,o,e,m=>{if(uc.has(u)){uc.delete(u);return}x_(n,u,c,m)})):t===3&&(uc.delete(u),gw(s,o,e,()=>{n.destroyNode(u)})),l!=null&&PN(n,t,e,l,o,i,a)}}function EN(t,n){gE(t,n),n[un]=null,n[fn]=null}function SN(t,n,e,i,r,o){i[un]=r,i[fn]=n,im(t,i,e,1,r,o)}function gE(t,n){n[Ni].changeDetectionScheduler?.notify(9),im(t,n,n[Ve],2,null,null)}function MN(t){let n=t[ps];if(!n)return lv(t[ne],t);for(;n;){let e=null;if(li(n))e=n[ps];else{let i=n[pt];i&&(e=i)}if(!e){for(;n&&!n[si]&&n!==t;)li(n)&&lv(n[ne],n),n=n[Dt];n===null&&(n=t),li(n)&&lv(n[ne],n),e=n&&n[si]}n=e}}function O_(t,n){let e=t[ba],i=e.indexOf(n);e.splice(i,1)}function nm(t,n){if(Eo(n))return;let e=n[Ve];e.destroyNode&&im(t,n,e,3,null,null),MN(n)}function lv(t,n){if(Eo(n))return;let e=ce(null);try{n[he]&=-129,n[he]|=256,n[Wn]&&mo(n[Wn]),AN(t,n),TN(t,n),n[ne].type===1&&n[Ve].destroy();let i=n[wo];if(i!==null&&qn(n[Dt])){i!==n[Dt]&&O_(i,n);let r=n[mr];r!==null&&r.detachView(t)}Dv(n)}finally{ce(e)}}function gw(t,n,e,i){let r=t?.[xo];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Sa.add(t[Vr]),hE(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:u}=c();s.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),IN(t,i)}else t&&Sa.delete(t[Vr]),i(!1)},r)}function IN(t,n){let e=t[xo]?.running;if(e){e.then(()=>{t[xo].running=void 0,Sa.delete(t[Vr]),n(!0)});return}n(!1)}function TN(t,n){let e=t.cleanup,i=n[ms];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[ms]=null);let r=n[Lr];if(r!==null){n[Lr]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[Co];if(o!==null){n[Co]=null;for(let a of o)a.destroy()}}function AN(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Ea)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];Be(Oe.LifecycleHookStart,s,l);try{l.call(s)}finally{Be(Oe.LifecycleHookEnd,s,l)}}else{Be(Oe.LifecycleHookStart,r,o);try{o.call(r)}finally{Be(Oe.LifecycleHookEnd,r,o)}}}}}function vE(t,n,e){return RN(t,n.parent,e)}function RN(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[un];if(hr(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Hi.None||r===Hi.Emulated)return null}return ci(i,e)}function _E(t,n,e){return ON(t,n,e)}function kN(t,n,e){return t.type&40?ci(t,e):null}var ON=kN,vw;function N_(t,n,e,i){let r=vE(t,i,n),o=n[Ve],a=i.parent||n[fn],s=_E(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)fw(o,r,e[l],s,!1);else fw(o,r,e,s,!1);vw!==void 0&&vw(o,i,n,e,r)}function fc(t,n){if(n!==null){let e=n.type;if(e&3)return ci(n,t);if(e&4)return Vv(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return fc(t,i);{let r=t[n.index];return qn(r)?Vv(-1,r):Yn(r)}}else{if(e&128)return fc(t,n.next);if(e&32)return A_(n,t)()||Yn(t[n.index]);{let i=yE(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Do(t[zt]);return fc(r,i)}else return fc(t,n.next)}}}return null}function yE(t,n){if(n!==null){let i=t[zt][fn],r=n.projection;return i.projection[r]}return null}function Vv(t,n){let e=pt+t+1;if(e<n.length){let i=n[e],r=i[ne].firstChild;if(r!==null)return fc(i,r)}return n[pr]}function P_(t,n,e,i,r,o,a){for(;e!=null;){let s=i[Gn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Ss(Yn(l),i),e.flags|=2),!Sc(e))if(c&8)P_(t,n,e.child,i,r,o,!1),Cs(n,t,s,r,l,e,o,i);else if(c&32){let u=A_(e,i),m;for(;m=u();)Cs(n,t,s,r,m,e,o,i);Cs(n,t,s,r,l,e,o,i)}else c&16?bE(t,n,i,e,r,o):Cs(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function im(t,n,e,i,r,o){P_(e,i,t.firstChild,n,r,o,!1)}function NN(t,n,e){let i=n[Ve],r=vE(t,e,n),o=e.parent||n[fn],a=_E(o,e,n);bE(i,0,n,e,r,a)}function bE(t,n,e,i,r,o){let a=e[zt],l=a[fn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Cs(n,t,e[Gn],r,u,i,o,e)}else{let c=l,u=a[Dt];Nf(i)&&(c.flags|=128),P_(t,n,c,u,r,o,!0)}}function PN(t,n,e,i,r,o,a){let s=i[pr],l=Yn(i);s!==l&&Cs(n,t,e,o,s,r,a);for(let c=pt;c<i.length;c++){let u=i[c];im(u[ne],u,t,n,o,s)}}function FN(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:yr.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=yr.Important),t.setStyle(e,i,r,o))}}function CE(t,n,e,i,r){let o=Li(),a=i&2;try{So(-1),a&&n.length>Ne&&mE(t,n,Ne,!1);let s=a?Oe.TemplateUpdateStart:Oe.TemplateCreateStart;Be(s,r,e),e(i,r)}finally{So(o);let s=a?Oe.TemplateUpdateEnd:Oe.TemplateCreateEnd;Be(s,r,e)}}function rm(t,n,e){UN(t,n,e),(e.flags&64)===64&&zN(t,n,e)}function Tc(t,n,e=ci){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function LN(t,n,e,i){let o=i.get(d_,Ex)||e===Hi.ShadowDom||e===Hi.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);if(a.tagName.toLowerCase()==="script")throw new P(905,!1);return VN(a),a}function VN(t){DE(t)}var DE=()=>null;function jN(t){cx(t)?nE(t):SO(t)}function wE(){DE=jN}function BN(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function xE(t,n,e,i,r,o){let a=n[ne];if(B_(t,a,n,e,i)){hr(t)&&HN(n,t.index);return}t.type&3&&(e=BN(e)),EE(t,n,e,i,r,o)}function EE(t,n,e,i,r,o){if(t.type&3){let a=ci(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function HN(t,n){let e=di(n,t);e[he]&16||(e[he]|=64)}function UN(t,n,e){let i=e.directiveStart,r=e.directiveEnd;hr(e)&&vN(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Of(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=pc(n,t,a,e);if(Ss(l,n),o!==null&&qN(n,a-i,l,s,e,o),Fi(s)){let c=di(e.index,n);c[wt]=pc(n,t,a,e)}}}function zN(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=jD();try{So(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];cf(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&$N(l,c)}}finally{So(-1),cf(a)}}function $N(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function F_(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];dE(n,o.selectors,!1)&&(i??=[],Fi(o)?i.unshift(o):i.push(o))}return i}function GN(t,n,e,i,r,o){let a=ci(t,n);WN(n[Ve],a,o,t.value,e,i,r)}function WN(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?dr(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function qN(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];Fv(i,e,l,c)}}function L_(t,n,e,i,r){let o=Ne+e,a=n[ne],s=r(a,n,t,i,e);n[o]=s,vs(t,!0);let l=t.type===2;return l?(iE(n[Ve],s,t),(RD()===0||hs(t))&&Ss(s,n),kD()):Ss(s,n),pf()&&(!l||!Sc(t))&&N_(a,n,s,t),t}function V_(t){let n=t;return qg()?Yg():(n=n.parent,vs(n,!1)),n}function j_(t,n){let e=t[Gn];if(!e)return;let i;try{i=e.get(Zn,null)}catch{i=null}i?.(n)}function B_(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],u=a[l+1],m=n.data[c];Fv(m,e[c],u,r),s=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];Fv(u,c,i,r),s=!0}return s}function YN(t,n){let e=di(n,t),i=e[ne];QN(i,e);let r=e[un];r!==null&&e[mn]===null&&(e[mn]=Bx(r,e[Gn])),Be(Oe.ComponentStart);try{H_(i,e,e[wt])}finally{Be(Oe.ComponentEnd,e[wt])}}function QN(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function H_(t,n,e){uf(n);try{let i=t.viewQuery;i!==null&&Mv(1,i,e);let r=t.template;r!==null&&CE(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[mr]?.finishViewCreation(t),t.staticContentQueries&&Wx(t,n),t.staticViewQueries&&Mv(2,t.viewQuery,e);let o=t.components;o!==null&&ZN(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[he]&=-5,ff()}}function ZN(t,n){for(let e=0;e<n.length;e++)YN(t,n[e])}function ks(t,n,e,i){let r=ce(null);try{let o=n.tView,s=t[he]&4096?4096:16,l=I_(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[wo]=c;let u=t[mr];return u!==null&&(l[mr]=u.createEmbeddedView(o)),H_(o,l,e),l}finally{ce(r)}}function Ma(t,n){return!n||n.firstChild===null||Nf(t)}function vc(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Yn(o)),qn(o)&&SE(o,i);let a=e.type;if(a&8)vc(t,n,e.child,i);else if(a&32){let s=A_(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=yE(n,e);if(Array.isArray(s))i.push(...s);else{let l=Do(n[zt]);vc(l[ne],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function SE(t,n){for(let e=pt;e<t.length;e++){let i=t[e],r=i[ne].firstChild;r!==null&&vc(i[ne],i,r,n)}t[pr]!==t[un]&&n.push(t[pr])}function ME(t){if(t[ya]!==null){for(let n of t[ya])n.impl.addSequence(n);t[ya].length=0}}var IE=[];function KN(t){return t[Wn]??XN(t)}function XN(t){let n=IE.pop()??Object.create(eP);return n.lView=t,n}function JN(t){t.lView[Wn]!==t&&(t.lView=null,IE.push(t))}var eP=fe(D({},co),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Da(t.lView)},consumerOnSignalRead(){this.lView[Wn]=this}});function tP(t){let n=t[Wn]??Object.create(nP);return n.lView=t,n}var nP=fe(D({},co),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Do(t.lView);for(;n&&!TE(n[ne]);)n=Do(n);n&&jg(n)},consumerOnSignalRead(){this.lView[Wn]=this}});function TE(t){return t.type!==2}function AE(t){if(t[Co]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Co])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[he]&8192)}}var iP=100;function RE(t,n=0){let i=t[Ni].rendererFactory,r=!1;r||i.begin?.();try{rP(t,n)}finally{r||i.end?.()}}function rP(t,n){let e=Qg();try{Zl(!0),jv(t,n);let i=0;for(;oc(t);){if(i===iP)throw new P(103,!1);i++,jv(t,1)}}finally{Zl(e)}}function oP(t,n,e,i){if(Eo(n))return;let r=n[he],o=!1,a=!1;uf(n);let s=!0,l=null,c=null;o||(TE(t)?(c=KN(n),l=Nr(c)):ou()===null?(s=!1,c=tP(n),l=Nr(c)):n[Wn]&&(mo(n[Wn]),n[Wn]=null));try{Vg(n),FD(t.bindingStartIndex),e!==null&&CE(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let _=t.preOrderCheckHooks;_!==null&&Df(n,_,null)}else{let _=t.preOrderHooks;_!==null&&wf(n,_,0,null),ov(n,0)}if(a||aP(n),AE(n),kE(n,0),t.contentQueries!==null&&Wx(t,n),!o)if(u){let _=t.contentCheckHooks;_!==null&&Df(n,_)}else{let _=t.contentHooks;_!==null&&wf(n,_,1),ov(n,1)}lP(t,n);let m=t.components;m!==null&&NE(n,m,0);let v=t.viewQuery;if(v!==null&&Mv(2,v,i),!o)if(u){let _=t.viewCheckHooks;_!==null&&Df(n,_)}else{let _=t.viewHooks;_!==null&&wf(n,_,2),ov(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[nf]){for(let _ of n[nf])_();n[nf]=null}o||(ME(n),n[he]&=-73)}catch(u){throw o||Da(n),u}finally{c!==null&&(fo(c,l),s&&JN(c)),ff()}}function kE(t,n){for(let e=fx(t);e!==null;e=mx(e))for(let i=pt;i<e.length;i++){let r=e[i];OE(r,n)}}function aP(t){for(let n=fx(t);n!==null;n=mx(n)){if(!(n[he]&2))continue;let e=n[ba];for(let i=0;i<e.length;i++){let r=e[i];jg(r)}}}function sP(t,n,e){Be(Oe.ComponentStart);let i=di(n,t);try{OE(i,e)}finally{Be(Oe.ComponentEnd,i[wt])}}function OE(t,n){of(t)&&jv(t,n)}function jv(t,n){let i=t[ne],r=t[he],o=t[Wn],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&es(o)),a||=!1,o&&(o.dirty=!1),t[he]&=-9217,a)oP(i,t,i.template,t[wt]);else if(r&8192){let s=ce(null);try{AE(t),kE(t,1);let l=i.components;l!==null&&NE(t,l,1),ME(t)}finally{ce(s)}}}function NE(t,n,e){for(let i=0;i<n.length;i++)sP(t,n[i],e)}function lP(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)So(~r);else{let o=r,a=e[++i],s=e[++i];VD(a,o);let l=n[o];Be(Oe.HostBindingsUpdateStart,l);try{s(2,l)}finally{Be(Oe.HostBindingsUpdateEnd,l)}}}}finally{So(-1)}}function om(t,n){let e=Qg()?64:1088;for(t[Ni].changeDetectionScheduler?.notify(n);t;){t[he]|=e;let i=Do(t);if(Ca(t)&&!i)return t;t=i}return null}function PE(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function FE(t,n){let e=pt+n;if(e<t.length)return t[e]}function Os(t,n,e,i=!0){let r=n[ne];if(cP(r,n,t,e),i){let a=Vv(e,t),s=n[Ve],l=s.parentNode(t[pr]);l!==null&&SN(r,t[fn],s,n,l,a)}let o=n[mn];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function U_(t,n){let e=_c(t,n);return e!==void 0&&nm(e[ne],e),e}function _c(t,n){if(t.length<=pt)return;let e=pt+n,i=t[e];if(i){let r=i[wo];r!==null&&r!==t&&O_(r,i),n>0&&(t[e-1][si]=i[si]);let o=nc(t,pt+n);EN(i[ne],i);let a=o[mr];a!==null&&a.detachView(o[ne]),i[Dt]=null,i[si]=null,i[he]&=-129}return i}function cP(t,n,e,i){let r=pt+i,o=e.length;i>0&&(e[r-1][si]=n),i<o-pt?(n[si]=e[r],Mg(e,pt+i,n)):(e.push(n),n[si]=null),n[Dt]=e;let a=n[wo];a!==null&&e!==a&&LE(a,n);let s=n[mr];s!==null&&s.insertView(t),af(n),n[he]|=128}function LE(t,n){let e=t[ba],i=n[Dt];if(li(i))t[he]|=2;else{let r=i[Dt][zt];n[zt]!==r&&(t[he]|=2)}e===null?t[ba]=[n]:e.push(n)}var Mo=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ne];return vc(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[wt]}set context(n){this._lView[wt]=n}get destroyed(){return Eo(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Dt];if(qn(n)){let e=n[rc],i=e?e.indexOf(this):-1;i>-1&&(_c(n,i),nc(e,i))}this._attachedToViewContainer=!1}nm(this._lView[ne],this._lView)}onDestroy(n){Bg(this._lView,n)}markForCheck(){om(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[he]&=-129}reattach(){af(this._lView),this._lView[he]|=128}detectChanges(){this._lView[he]|=1024,RE(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new P(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ca(this._lView),e=this._lView[wo];e!==null&&!n&&O_(e,this._lView),gE(this._lView[ne],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new P(902,!1);this._appRef=n;let e=Ca(this._lView),i=this._lView[wo];i!==null&&!e&&LE(i,this._lView),af(this._lView)}};var je=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=dP;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=ks(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Mo(o)}}return t})();function dP(){return am(Pt(),se())}function am(t,n){return t.type&4?new je(n,t,Ts(t,n)):null}function Ns(t,n,e,i,r){let o=t.data[n];if(o===null)o=uP(t,n,e,i,r),LD()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=ND();o.injectorIndex=a===null?-1:a.injectorIndex}return vs(o,!0),o}function uP(t,n,e,i,r){let o=Wg(),a=qg(),s=a?o:o&&o.parent,l=t.data[n]=mP(t,s,e,n,i,r);return fP(t,l,o,a),l}function fP(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function mP(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return lf()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:mf(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var pP=new RegExp(`^(\\d+)*(${vx}|${gx})*(.*)`);function hP(t){let n=t.match(pP),[e,i,r,o]=n,a=i?parseInt(i,10):r,s=[];for(let[l,c,u]of o.matchAll(/(f|n)(\d*)/g)){let m=parseInt(u,10)||1;s.push(c,m)}return[a,...s]}function gP(t){return!t.prev&&t.parent?.type===8}function cv(t){return t.index-Ne}function vP(t,n){let e=t.i18nNodes;if(e)return e.get(n)}function sm(t,n,e,i){let r=cv(i),o=vP(t,r);if(o===void 0){let a=t.data[Cx];if(a?.[r])o=yP(a[r],e);else if(n.firstChild===i)o=t.firstChild;else{let s=i.prev===null,l=i.prev??i.parent;if(gP(i)){let c=cv(i.parent);o=Sv(t,c)}else{let c=ci(l,e);if(s)o=c.firstChild;else{let u=cv(l),m=Sv(t,u);if(l.type===2&&m){let _=h_(t,u)+1;o=lm(_,m)}else o=c.nextSibling}}}}return o}function lm(t,n){let e=n;for(let i=0;i<t;i++)e=e.nextSibling;return e}function _P(t,n){let e=t;for(let i=0;i<n.length;i+=2){let r=n[i],o=n[i+1];for(let a=0;a<o;a++)switch(r){case _O:e=e.firstChild;break;case yO:e=e.nextSibling;break}}return e}function yP(t,n){let[e,...i]=hP(t),r;if(e===gx)r=n[zt][un];else if(e===vx)r=sE(n[zt][un]);else{let o=Number(e);r=Yn(n[o+Ne])}return _P(r,i)}var bP=!1;function VE(t){bP=t}function CP(t){let n=t[mn];if(n){let{i18nNodes:e,dehydratedIcuData:i}=n;if(e&&i){let r=t[Ve];for(let o of i.values())DP(r,e,o)}n.i18nNodes=void 0,n.dehydratedIcuData=void 0}}function DP(t,n,e){for(let i of e.node.cases[e.case]){let r=n.get(i.index-Ne);r&&x_(t,r,!1)}}function cm(t){let n=t[Pi]??[],i=t[Dt][Ve],r=[];for(let o of n)o.data[Dx]!==void 0?r.push(o):jE(o,i);t[Pi]=r}function wP(t){let{lContainer:n}=t,e=n[Pi];if(e===null)return;let r=n[Dt][Ve];for(let o of e)jE(o,r)}function jE(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[hc];for(;e<r;){let o=i.nextSibling;x_(n,i,!1),i=o,e++}}}function dm(t){cm(t);let n=t[un];li(n)&&Ff(n);for(let e=pt;e<t.length;e++)Ff(t[e])}function Ff(t){CP(t);let n=t[ne];for(let e=Ne;e<n.bindingStartIndex;e++)if(qn(t[e])){let i=t[e];dm(i)}else li(t[e])&&Ff(t[e])}function z_(t){let n=t._views;for(let e of n){let i=Hx(e);i!==null&&i[un]!==null&&(li(i)?Ff(i):dm(i))}}function xP(t,n,e,i){t!==null&&(e.cleanup(n),dm(t.lContainer),z_(i))}function EP(t,n){let e=[];for(let i of n)for(let r=0;r<(i[l_]??1);r++){let o={data:i,firstChild:null};i[hc]>0&&(o.firstChild=t,t=lm(i[hc],t)),e.push(o)}return[t,e]}var BE=()=>null,HE=()=>null;function UE(){BE=SP,HE=MP}function SP(t,n){return $E(t,n)?t[Pi].shift():(cm(t),null)}function yc(t,n){return BE(t,n)}function MP(t,n,e){if(n.tView.ssrId===null)return null;let i=yc(t,n.tView.ssrId);return e[ne].firstUpdatePass&&i===null&&IP(e,n),i}function zE(t,n,e){return HE(t,n,e)}function IP(t,n){let e=n;for(;e;){if(_w(t,e))return;if((e.flags&256)===256)break;e=e.prev}for(e=n.next;e&&(e.flags&512)===512;){if(_w(t,e))return;e=e.next}}function $E(t,n){let e=t[Pi];return!n||e===null||e.length===0?!1:e[0].data[bx]===n}function _w(t,n){let e=n.tView?.ssrId;if(e==null)return!1;let i=t[n.index];return qn(i)&&$E(i,e)?(cm(i),!0):!1}var GE=class{},um=class{},Bv=class{resolveComponentFactory(n){throw new P(917,!1)}},Ac=class{static NULL=new Bv},Tt=class{},Ye=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>TP()}return t})();function TP(){let t=se(),n=Pt(),e=di(n.index,t);return(li(e)?e:t)[Ve]}var WE=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>null})}return t})();var Sf={},ws=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Sf,i);return r!==Sf||e===Sf?r:this.parentInjector.get(n,e,i)}};function Lf(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=Yu(r,s);else if(o==2){let l=s,c=n[++a];i=Yu(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function J(t,n=0){let e=se();if(e===null)return le(t,n);let i=Pt();return ix(i,e,Nt(t),n)}function fm(){let t="invalid";throw new Error(t)}function qE(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let u of a)if(u.resolveHostDirectives!==null){[s,l,c]=u.resolveHostDirectives(a);break}kP(t,n,e,s,o,l,c)}o!==null&&i!==null&&AP(e,i,o)}function AP(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new P(-301,!1);i.push(n[r],o)}}function RP(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function kP(t,n,e,i,r,o,a){let s=i.length,l=null;for(let v=0;v<s;v++){let _=i[v];l===null&&Fi(_)&&(l=_,RP(t,e,v)),bv(Of(e,n),t,_.type)}VP(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let v=0;v<s;v++){let _=i[v];_.providersResolver&&_.providersResolver(_)}let c=!1,u=!1,m=fE(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let v=0;v<s;v++){let _=i[v];if(e.mergedAttrs=Es(e.mergedAttrs,_.hostAttrs),NP(t,e,n,m,_),LP(m,_,r),a!==null&&a.has(_)){let[A,R]=a.get(_);e.directiveToIndex.set(_.type,[m,A+e.directiveStart,R+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,m);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let b=_.type.prototype;!c&&(b.ngOnChanges||b.ngOnInit||b.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(b.ngOnChanges||b.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),m++}OP(t,e,o)}function OP(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))yw(0,n,r,i),yw(1,n,r,i),Cw(n,i,!1);else{let o=e.get(r);bw(0,n,o,i),bw(1,n,o,i),Cw(n,i,!0)}}}function yw(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),YE(n,o)}}function bw(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),YE(n,a)}}function YE(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Cw(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||S_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){a??=[],a.push(c[u+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function NP(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=bo(r.type,!0)),a=new Ea(o,Fi(r),J,null);t.blueprint[i]=a,e[i]=a,PP(t,n,i,fE(t,e,r.hostVars,Ht),r)}function PP(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;FP(a)!=s&&a.push(s),a.push(e,i,o)}}function FP(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function LP(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Fi(n)&&(e[""]=t)}}function VP(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function $_(t,n,e,i,r,o,a,s){let l=n[ne],c=l.consts,u=Qn(c,a),m=Ns(l,t,e,i,u);return o&&qE(l,n,m,Qn(c,s),r),m.mergedAttrs=Es(m.mergedAttrs,m.attrs),m.attrs!==null&&Lf(m,m.attrs,!1),m.mergedAttrs!==null&&Lf(m,m.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,m),m}function G_(t,n){Yw(t,n),Ng(n)&&t.queries.elementEnd(n)}function jP(t,n,e,i,r,o){let a=n.consts,s=Qn(a,r),l=Ns(n,t,e,i,s);if(l.mergedAttrs=Es(l.mergedAttrs,l.attrs),o!=null){let c=Qn(a,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Lf(l,l.attrs,!1),l.mergedAttrs!==null&&Lf(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function W_(t){return ZE(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function QE(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function ZE(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function mm(t,n,e){return t[n]=e}function BP(t,n){return t[n]}function kn(t,n,e){if(e===Ht)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function q_(t,n,e,i){let r=kn(t,n,e);return kn(t,n+1,i)||r}function KE(t,n,e,i,r){let o=q_(t,n,e,i);return kn(t,n+2,r)||o}function Mf(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&bO(r,o);let a=hr(t)?di(t.index,n):n;om(a,5);let s=n[wt],l=Dw(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=Dw(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function Dw(t,n,e,i){let r=ce(null);try{return Be(Oe.OutputStart,n,e),e(i)!==!1}catch(o){return j_(t,o),!1}finally{Be(Oe.OutputEnd,n,e),ce(r)}}function XE(t,n,e,i,r,o,a,s){let l=hs(t),c=!1,u=null;if(!i&&l&&(u=UP(n,e,o,t.index)),u!==null){let m=u.__ngLastListenerFn__||u;m.__ngNextListenerFn__=a,u.__ngLastListenerFn__=a,c=!0}else{let m=ci(t,e),v=i?i(m):m;CO(e,v,o,s),i||(s.__ngNativeEl__=m);let _=r.listen(v,o,s);if(!HP(o)){let b=i?A=>i(Yn(A[t.index])):t.index;JE(b,n,e,o,s,_,!1)}}return c}function HP(t){return t.startsWith("animation")||t.startsWith("transition")}function UP(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[ms],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function JE(t,n,e,i,r,o,a){let s=n.firstCreatePass?Ug(n):null,l=Hg(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function ww(t,n,e,i,r,o){let a=n[e],s=n[ne],c=s.data[e].outputs[i],m=a[c].subscribe(o);JE(t.index,s,n,r,o,m,!0)}var Hv=Symbol("BINDING");function eS(t){return t.debugInfo?.className||t.type.name||null}var Vf=class extends Ac{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=ki(n);return new Io(e,this.ngModule)}};function zP(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Jf.SignalBased)!==0};return r&&(o.transform=r),o})}function $P(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function GP(t,n,e){let i=n instanceof We?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new ws(e,i):e}function WP(t){let n=t.get(Tt,null);if(n===null)throw new P(407,!1);let e=t.get(WE,null),i=t.get(Ri,null),r=t.get(zi,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function qP(t,n){let e=tS(t);return w_(n,e,e==="svg"?Pg:e==="math"?ED:null)}function tS(t){return(t.selectors[0][0]||"div").toLowerCase()}var Io=class extends um{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=zP(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=$P(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=mN(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){Be(Oe.DynamicComponentStart);let s=ce(null);try{let l=this.componentDef,c=GP(l,r||this.ngModule,n),u=WP(c),m=u.tracingService;return m&&m.componentCreate?m.componentCreate(eS(l),()=>this.createComponentRef(u,c,e,i,o,a)):this.createComponentRef(u,c,e,i,o,a)}finally{ce(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=YP(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),u=r?LN(c,r,s.encapsulation,e):qP(s,c),m=a?.some(xw)||o?.some(b=>typeof b!="function"&&b.bindings.some(xw)),v=I_(null,l,null,512|uE(s),null,null,n,c,e,null,Bx(u,e,!0));v[Ne]=u,uf(v);let _=null;try{let b=$_(Ne,v,2,"#host",()=>l.directiveRegistry,!0,0);iE(c,u,b),Ss(u,v),rm(l,v,b),g_(l,b,v),G_(l,b),i!==void 0&&ZP(b,this.ngContentSelectors,i),_=di(b.index,v),v[wt]=_[wt],H_(l,v,null)}catch(b){throw _!==null&&Dv(_),Dv(v),b}finally{Be(Oe.DynamicComponentEnd),ff()}return new jf(this.componentType,v,!!m)}};function YP(t,n,e,i){let r=t?["ng-version","21.2.15"]:pN(n.selectors[0]),o=null,a=null,s=0;if(e)for(let u of e)s+=u[Hv].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(a??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let m=i[u];if(typeof m!="function")for(let v of m.bindings){s+=v[Hv].requiredVars;let _=u+1;v.create&&(v.targetIdx=_,(o??=[]).push(v)),v.update&&(v.targetIdx=_,(a??=[]).push(v))}}let l=[n];if(i)for(let u of i){let m=typeof u=="function"?u:u.type,v=tc(m);l.push(v)}return M_(0,null,QP(o,a),1,s,l,null,null,null,[r],null)}function QP(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function xw(t){let n=t[Hv].kind;return n==="input"||n==="twoWay"}var jf=class extends GE{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=gs(e[ne],Ne),this.location=Ts(this._tNode,e),this.instance=di(this._tNode.index,e)[wt],this.hostView=this.changeDetectorRef=new Mo(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=B_(i,r[ne],r,n,e);this.previousInputValues.set(n,e);let a=di(i.index,r);om(a,1)}get injector(){return new xa(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function ZP(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var ct=(()=>{class t{static __NG_ELEMENT_ID__=KP}return t})();function KP(){let t=Pt();return nS(t,se())}var Uv=class t extends ct{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Ts(this._hostTNode,this._hostLView)}get injector(){return new xa(this._hostTNode,this._hostLView)}get parentInjector(){let n=o_(this._hostTNode,this._hostLView);if(Kw(n)){let e=Rf(n,this._hostLView),i=Af(n),r=e[ne].data[i+8];return new xa(r,e)}else return new xa(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Ew(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-pt}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=yc(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,Ma(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!G1(n),c;if(l)c=e;else{let R=e||{};c=R.index,i=R.injector,r=R.projectableNodes,o=R.environmentInjector||R.ngModuleRef,a=R.directives,s=R.bindings}let u=l?n:new Io(ki(n)),m=i||this.parentInjector;if(!o&&u.ngModule==null){let L=(l?m:this.parentInjector).get(We,null);L&&(o=L)}let v=ki(u.componentType??{}),_=yc(this._lContainer,v?.id??null),b=_?.firstChild??null,A=u.create(m,r,b,o,a,s);return this.insertImpl(A.hostView,c,Ma(this._hostTNode,_)),A}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(MD(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[Dt],c=new t(l,l[fn],l[Dt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return Os(a,r,o,i),n.attachToViewContainerRef(),Mg(dv(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Ew(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=_c(this._lContainer,e);i&&(nc(dv(this._lContainer),e),nm(i[ne],i))}detach(n){let e=this._adjustIndex(n,-1),i=_c(this._lContainer,e);return i&&nc(dv(this._lContainer),e)!=null?new Mo(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Ew(t){return t[rc]}function dv(t){return t[rc]||(t[rc]=[])}function nS(t,n){let e,i=n[t.index];return qn(i)?e=i:(e=PE(i,n,null,t),n[t.index]=e,T_(n,e)),iS(e,n,t,i),new Uv(e,t,n)}function XP(t,n){let e=t[Ve],i=e.createComment(""),r=ci(n,t),o=e.parentNode(r);return Pf(e,o,i,e.nextSibling(r),!1),i}var iS=rS,Y_=()=>!1;function JP(t,n,e){return Y_(t,n,e)}function rS(t,n,e,i){if(t[pr])return;let r;e.type&8?r=Yn(i):r=XP(n,e),t[pr]=r}function eF(t,n,e){if(t[pr]&&t[Pi])return!0;let i=e[mn],r=n.index-Ne;if(!i||dx(n)||$x(i,r))return!1;let a=Sv(i,r),s=i.data[Yf]?.[r];if(s===void 0)return!1;let[l,c]=EP(a,s);return t[pr]=l,t[Pi]=c,!0}function tF(t,n,e,i){Y_(t,e,n)||rS(t,n,e,i)}function oS(){iS=tF,Y_=eF}var zv=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},$v=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Z_(n,e).matches!==null&&this.queries[e].setDirty()}},Bf=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=aF(n):this.predicate=n}},Gv=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Wv=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,nF(e,o)),this.matchTNodeWithReadOption(n,e,xf(e,n,o,!1,!1))}else i===je?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,xf(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===G||r===ct||r===je&&e.type&4)this.addMatch(e.index,-2);else{let o=xf(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function nF(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function iF(t,n){return t.type&11?Ts(t,n):t.type&4?am(t,n):null}function rF(t,n,e,i){return e===-1?iF(n,t):e===-2?oF(t,n,i):pc(t,t[ne],e,n)}function oF(t,n,e){if(e===G)return Ts(n,t);if(e===je)return am(n,t);if(e===ct)return nS(n,t)}function aS(t,n,e,i){let r=n[mr].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let u=o[c];s.push(rF(n,u,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function qv(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=aS(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],u=n[-l];for(let m=pt;m<u.length;m++){let v=u[m];v[wo]===v[Dt]&&qv(v[ne],v,c,i)}if(u[ba]!==null){let m=u[ba];for(let v=0;v<m.length;v++){let _=m[v];qv(_[ne],_,c,i)}}}}}return i}function Q_(t,n){return t[mr].queries[n].queryList}function sS(t,n,e){let i=new Jt((e&4)===4);return AD(t,n,i,i.destroy),(n[mr]??=new $v).queries.push(new zv(i))-1}function lS(t,n,e){let i=nt();return i.firstCreatePass&&(dS(i,new Bf(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),sS(i,se(),n)}function cS(t,n,e,i){let r=nt();if(r.firstCreatePass){let o=Pt();dS(r,new Bf(n,e,i),o.index),sF(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return sS(r,se(),e)}function aF(t){return t.split(",").map(n=>n.trim())}function dS(t,n,e){t.queries===null&&(t.queries=new Gv),t.queries.track(new Wv(n,e))}function sF(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Z_(t,n){return t.queries.getByIndex(n)}function uS(t,n){let e=t[ne],i=Z_(e,n);return i.crossesNgTemplate?qv(e,t,n,[]):aS(e,t,i,n)}function fS(t,n,e){let i,r=Nl(()=>{i._dirtyCounter();let o=lF(i,t);if(n&&o===void 0)throw new P(-951,!1);return o});return i=r[bt],i._dirtyCounter=S(0),i._flatValue=void 0,r}function K_(t){return fS(!0,!1,t)}function X_(t){return fS(!0,!0,t)}function mS(t,n){let e=t[bt];e._lView=se(),e._queryIndex=n,e._queryList=Q_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function lF(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[he]&4)return n?void 0:Xt;let r=Q_(e,i),o=uS(e,i);return r.reset(o,ax),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var br=class{},pm=class{};var Hf=class extends br{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Vf(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=wg(n);this._bootstrapComponents=lE(o.bootstrap),this._r3Injector=Jg(n,e,[{provide:br,useValue:this},{provide:Ac,useValue:this.componentFactoryResolver},...i],Jl(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Uf=class extends pm{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Hf(this.moduleType,n,[])}};var bc=class extends br{injector;componentFactoryResolver=new Vf(this);instance=null;constructor(n){super();let e=new ha([...n.providers,{provide:br,useValue:this},{provide:Ac,useValue:this.componentFactoryResolver}],n.parent||fs(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Ps(t,n,e=null){return new bc({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var cF=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=tf(!1,e.type),r=i.length>0?Ps([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t(le(We))})}return t})();function O(t){return Dc(()=>{let n=pS(t),e=fe(D({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===a_.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(cF).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Hi.Emulated,styles:t.styles||Xt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&fi("NgStandalone"),hS(e);let i=t.dependencies;return e.directiveDefs=Sw(i,dF),e.pipeDefs=Sw(i,xg),e.id=mF(e),e})}function dF(t){return ki(t)||tc(t)}function H(t){return Dc(()=>({type:t.type,bootstrap:t.bootstrap||Xt,declarations:t.declarations||Xt,imports:t.imports||Xt,exports:t.exports||Xt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function uF(t,n){if(t==null)return Oi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=Jf.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function fF(t){if(t==null)return Oi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function M(t){return Dc(()=>{let n=pS(t);return hS(n),n})}function hm(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function pS(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Oi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Xt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:uF(t.inputs,n),outputs:fF(t.outputs),debugInfo:null}}function hS(t){t.features?.forEach(n=>n(t))}function Sw(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function mF(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function J_(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=pF,e.hostDirectives=i?t.map(Yv):[t]):i?e.hostDirectives.unshift(...t.map(Yv)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function pF(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,gS(a,n,i),r.set(a,[s,n.length-1])}o===0&&Fi(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function gS(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)Mw(Yv(o),n,e)}else Mw(i,n,e)}function Mw(t,n,e){let i=tc(t.directive);hF(i.declaredInputs,t.inputs),gS(i,n,e),e.set(i,t),n.push(i)}function Yv(t){return typeof t=="function"?{directive:Nt(t),inputs:Oi,outputs:Oi}:{directive:Nt(t.directive),inputs:Iw(t.inputs),outputs:Iw(t.outputs)}}function Iw(t){if(t===void 0||t.length===0)return Oi;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function hF(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function gF(t){return Object.getPrototypeOf(t.prototype).constructor}function ae(t){let n=gF(t.type),e=!0,i=[t];for(;n;){let r;if(Fi(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new P(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let a=t;a.inputs=uv(t.inputs),a.declaredInputs=uv(t.declaredInputs),a.outputs=uv(t.outputs);let s=r.hostBindings;s&&CF(t,s);let l=r.viewQuery,c=r.contentQueries;if(l&&yF(t,l),c&&bF(t,c),vF(t,r),dD(t.outputs,r.outputs),Fi(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let a=0;a<o.length;a++){let s=o[a];s&&s.ngInherit&&s(t),s===ae&&(e=!1)}}n=Object.getPrototypeOf(n)}_F(i)}function vF(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function _F(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Es(r.hostAttrs,e=Es(e,r.hostAttrs))}}function uv(t){return t===Oi?{}:t===Xt?[]:t}function yF(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function bF(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function CF(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function vS(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=Es(t.mergedAttrs,t.attrs);let u=t.tView=M_(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),vs(t,!1);let l=_S(e,n,t,i);pf()&&N_(e,n,l,t),Ss(l,n);let c=PE(l,n,l,t);n[i+Ne]=c,T_(n,c),JP(c,t,n)}function DF(t,n,e,i,r,o,a,s,l,c,u){let m=e+Ne,v;return n.firstCreatePass?(v=Ns(n,m,4,a||null,s||null),sf()&&qE(n,t,v,Qn(n.consts,c),F_),Yw(n,v)):v=n.data[m],vS(v,t,n,e,i,r,o,l),hs(v)&&rm(n,t,v),c!=null&&Tc(t,v,u),v}function Ms(t,n,e,i,r,o,a,s,l,c,u){let m=e+Ne,v;if(n.firstCreatePass){if(v=Ns(n,m,4,a||null,s||null),c!=null){let _=Qn(n.consts,c);v.localNames=[];for(let b=0;b<_.length;b+=2)v.localNames.push(_[b],-1)}}else v=n.data[m];return vS(v,t,n,e,i,r,o,l),c!=null&&Tc(t,v,u),v}function N(t,n,e,i,r,o,a,s){let l=se(),c=nt(),u=Qn(c.consts,o);return DF(l,c,t,n,e,i,r,u,void 0,a,s),N}function Fs(t,n,e,i,r,o,a,s){let l=se(),c=nt(),u=Qn(c.consts,o);return Ms(l,c,t,n,e,i,r,u,void 0,a,s),Fs}var _S=yS;function yS(t,n,e,i){return Br(!0),n[Ve].createComment("")}function wF(t,n,e,i){let r=!Kf(n,e);Br(r);let o=n[mn]?.data[yx]?.[i]??null;if(o!==null&&e.tView!==null&&e.tView.ssrId===null&&(e.tView.ssrId=o),r)return yS(t,n);let a=n[mn],s=sm(a,t,n,e);Zf(a,i,s);let l=h_(a,i);return lm(l,s)}function bS(){_S=wF}var Kn=(function(t){return t[t.NOT_STARTED=0]="NOT_STARTED",t[t.IN_PROGRESS=1]="IN_PROGRESS",t[t.COMPLETE=2]="COMPLETE",t[t.FAILED=3]="FAILED",t})(Kn||{}),Tw=0,xF=1,It=(function(t){return t[t.Placeholder=0]="Placeholder",t[t.Loading=1]="Loading",t[t.Complete=2]="Complete",t[t.Error=3]="Error",t})(It||{});var EF=0,Rc=1;var SF=4,MF=5;var IF=7,xs=8,TF=9,ey=(function(t){return t[t.Manual=0]="Manual",t[t.Playthrough=1]="Playthrough",t})(ey||{});function If(t,n){let e=RF(t),i=n[e];if(i!==null){for(let r of i)r();n[e]=null}}function AF(t){If(1,t),If(0,t),If(2,t)}function RF(t){let n=SF;return t===1?n=MF:t===2&&(n=TF),n}function CS(t){return t+1}function Ls(t,n){let e=t[ne],i=CS(n.index);return t[i]}function kc(t,n){let e=CS(n.index);return t.data[e]}function kF(t,n,e){let i=n[ne],r=kc(i,e);switch(t){case It.Complete:return r.primaryTmplIndex;case It.Loading:return r.loadingTmplIndex;case It.Error:return r.errorTmplIndex;case It.Placeholder:return r.placeholderTmplIndex;default:return null}}function Aw(t,n){return n===It.Placeholder?t.placeholderBlockConfig?.[Tw]??null:n===It.Loading?t.loadingBlockConfig?.[Tw]??null:null}function OF(t){return t.loadingBlockConfig?.[xF]??null}function Rw(t,n){if(!t||t.length===0)return n;let e=new Set(t);for(let i of n)e.add(i);return t.length===e.size?t:Array.from(e)}function NF(t,n){let e=n.primaryTmplIndex+Ne;return gs(t,e)}var PF=(()=>{class t{cachedInjectors=new Map;getOrCreateInjector(e,i,r,o){if(!this.cachedInjectors.has(e)){let a=r.length>0?Ps(r,i,o):null;this.cachedInjectors.set(e,a)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t})}return t})();var DS=new C("");function fv(t,n,e){return t.get(PF).getOrCreateInjector(n,t,e,"")}function FF(t,n,e){if(t instanceof ws){let r=t.injector,o=t.parentInjector,a=fv(o,n,e);return new ws(r,a)}let i=t.get(We);if(i!==t){let r=fv(i,n,e);return new ws(t,r)}return fv(t,n,e)}function wa(t,n,e,i=!1){let r=e[Dt],o=r[ne];if(Eo(r))return;let a=Ls(r,n),s=a[Rc],l=a[IF];if(!(l!==null&&t<l)&&kw(s,t)&&kw(a[EF]??-1,t)){let c=kc(o,n),m=!i&&!0&&(OF(c)!==null||Aw(c,It.Loading)!==null||Aw(c,It.Placeholder))?jF:VF;try{m(t,a,e,n,r)}catch(v){j_(r,v)}}}function LF(t,n){let e=t[Pi]?.findIndex(r=>r.data[wx]===n[Rc])??-1;return{dehydratedView:e>-1?t[Pi][e]:null,dehydratedViewIx:e}}function VF(t,n,e,i,r){Be(Oe.DeferBlockStateStart);let o=kF(t,r,i);if(o!==null){n[Rc]=t;let a=r[ne],s=o+Ne,l=gs(a,s),c=0;U_(e,c);let u;if(t===It.Complete){let b=kc(a,i),A=b.providers;A&&A.length>0&&(u=FF(r[Gn],b,A))}let{dehydratedView:m,dehydratedViewIx:v}=LF(e,n),_=ks(r,l,null,{injector:u,dehydratedView:m});if(Os(e,_,c,Ma(l,m)),om(_,2),v>-1&&e[Pi]?.splice(v,1),(t===It.Complete||t===It.Error)&&Array.isArray(n[xs])){for(let b of n[xs])b();n[xs]=null}}Be(Oe.DeferBlockStateEnd)}function kw(t,n){return t<n}function Ow(t,n,e){t.loadingPromise.then(()=>{t.loadingState===Kn.COMPLETE?wa(It.Complete,n,e):t.loadingState===Kn.FAILED&&wa(It.Error,n,e)})}var jF=null;var gm=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function Cr(t){return typeof t=="function"&&t[bt]!==void 0}function ty(t){return Cr(t)&&typeof t.set=="function"}var ny=new C("");function To(t){return!!t&&typeof t.then=="function"}function iy(t){return!!t&&typeof t.subscribe=="function"}var wS=new C("");var ry=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(wS,{optional:!0})??[];injector=d(oe);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Bt(this.injector,r);if(To(o))e.push(o);else if(iy(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ao=new C("");function xS(){Vh(()=>{let t="";throw new P(600,t)})}function ES(t){return t.isBoundToModule}var BF=10;var At=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Zn);afterRenderManager=d(tm);zonelessEnabled=d(cc);rootEffectScheduler=d(gf);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new k;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(vr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(pe(e=>!e))}constructor(){d(zi,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(We);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=oe.NULL){return this._injector.get(ie).run(()=>{Be(Oe.BootstrapComponentStart);let a=e instanceof um;if(!this._injector.get(ry).done){let b="";throw new P(405,b)}let l;a?l=e:l=this._injector.get(Ac).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=ES(l)?void 0:this._injector.get(br),u=i||l.selector,m=l.create(r,[],u,c),v=m.location.nativeElement,_=m.injector.get(ny,null);return _?.registerApplication(v),m.onDestroy(()=>{this.detachView(m.hostView),mc(this.components,m),_?.unregisterApplication(v)}),this._loadComponent(m),Be(Oe.BootstrapComponentEnd,m),m})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Be(Oe.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(em.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Be(Oe.ChangeDetectionEnd),new P(101,!1);let e=ce(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ce(e),this.afterTick.next(),Be(Oe.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Tt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<BF;){Be(Oe.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Be(Oe.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!oc(r))continue;let o=i&&!this.zonelessEnabled?0:1;RE(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>oc(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;mc(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Ao,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>mc(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new P(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function mc(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function oy(){let t,n;return{promise:new Promise((i,r)=>{t=i,n=r}),resolve:t,reject:n}}function SS(t,n,e){let i=n[Gn],r=n[ne];if(t.loadingState!==Kn.NOT_STARTED)return t.loadingPromise??Promise.resolve();let o=Ls(n,e),a=NF(r,t);t.loadingState=Kn.IN_PROGRESS,If(1,o);let s=t.dependencyResolverFn,l=i.get(bs).add();return s?(t.loadingPromise=Promise.allSettled(s()).then(c=>{let u=!1,m=null,v=[],_=[];for(let b=0;b<c.length;b++){let A=c[b];if(A.status==="fulfilled"){let R=A.value,L=ki(R)||tc(R);if(L)v.push(L);else{let _e=xg(R);_e&&_.push(_e)}}else{u=!0,m=A.reason instanceof Error?A.reason:new Error(String(A.reason));break}}if(u){if(t.loadingState=Kn.FAILED,t.errorTmplIndex===null){let A="",R=new P(-750,A);j_(n,R)}}else{t.loadingState=Kn.COMPLETE;let b=a.tView;if(v.length>0){b.directiveRegistry=Rw(b.directiveRegistry,v);let A=v.map(L=>L.type),R=tf(!1,...A);t.providers=R}_.length>0&&(b.pipeRegistry=Rw(b.pipeRegistry,_))}}),t.loadingPromise.finally(()=>{t.loadingPromise=null,l()})):(t.loadingPromise=Promise.resolve().then(()=>{t.loadingPromise=null,t.loadingState=Kn.COMPLETE,l()}),t.loadingPromise)}function HF(t,n){return n[Gn].get(DS,null,{optional:!0})?.behavior!==ey.Manual}function UF(t,n,e){let i=n[ne],r=n[e.index];if(!HF(t,n))return;let o=Ls(n,e),a=kc(i,e);switch(AF(o),a.loadingState){case Kn.NOT_STARTED:wa(It.Loading,e,r),SS(a,n,e),a.loadingState===Kn.IN_PROGRESS&&Ow(a,e,r);break;case Kn.IN_PROGRESS:wa(It.Loading,e,r),Ow(a,e,r);break;case Kn.COMPLETE:wa(It.Complete,e,r);break;case Kn.FAILED:wa(It.Error,e,r);break;default:}}async function MS(t,n,e){let i=t.get(Qf);if(i.hydrating.has(n))return;let{parentBlockPromise:o,hydrationQueue:a}=TO(n,t);if(a.length===0)return;o!==null&&a.shift(),GF(i,a),o!==null&&await o;let s=a[0];i.has(s)?await Nw(t,a,e):i.awaitParentBlock(s,async()=>await Nw(t,a,e))}async function Nw(t,n,e){let i=t.get(Qf),r=i.hydrating,o=t.get(vr),a=o.add();for(let l=0;l<n.length;l++){let c=n[l],u=i.get(c);if(u!=null){if(await qF(u),await WF(t),zF(u)){wP(u),Pw(n.slice(l),i);break}r.get(c).resolve()}else{$F(l,n,i),Pw(n.slice(l),i);break}}let s=n[n.length-1];await r.get(s)?.promise,o.remove(a),e&&e(n),xP(i.get(s),n,i,t.get(At))}function zF(t){return Ls(t.lView,t.tNode)[Rc]===It.Error}function $F(t,n,e){let i=t-1,r=i>-1?e.get(n[i]):null;r&&dm(r.lContainer)}function Pw(t,n){let e=n.hydrating;for(let i in t)e.get(i)?.reject();n.cleanup(t)}function GF(t,n){for(let e of n)t.hydrating.set(e,oy())}function WF(t){return new Promise(n=>ht(n,{injector:t}))}async function qF(t){let{tNode:n,lView:e}=t,i=Ls(e,n);return new Promise(r=>{YF(i,r),UF(2,e,n)})}function YF(t,n){Array.isArray(t[xs])||(t[xs]=[]),t[xs].push(n)}function de(t,n,e,i){let r=se(),o=jr();if(kn(r,o,n)){let a=nt(),s=lc();GN(s,r,t,n,e,i)}return de}var Qv=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function mv(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function QF(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){ce(i);let c=n.length-1;for(ce(null);a<=s&&a<=c;){let u=t.at(a),m=n[a],v=mv(a,u,a,m,e);if(v!==0){v<0&&t.updateValue(a,m),a++;continue}let _=t.at(s),b=n[c],A=mv(s,_,c,b,e);if(A!==0){A<0&&t.updateValue(s,b),s--,c--;continue}let R=e(a,u),L=e(s,_),_e=e(a,m);if(Object.is(_e,L)){let yt=e(c,b);Object.is(yt,R)?(t.swap(a,s),t.updateValue(s,b),c--,s--):t.move(s,a),t.updateValue(a,m),a++;continue}if(r??=new zf,o??=Lw(t,a,s,e),Zv(t,r,a,_e))t.updateValue(a,m),a++,s++;else if(o.has(_e))r.set(R,t.detach(a)),s--;else{let yt=t.create(a,n[a]);t.attach(a,yt),a++,s++}}for(;a<=c;)Fw(t,r,e,a,n[a]),a++}else if(n!=null){ce(i);let c=n[Symbol.iterator]();ce(null);let u=c.next();for(;!u.done&&a<=s;){let m=t.at(a),v=u.value,_=mv(a,m,a,v,e);if(_!==0)_<0&&t.updateValue(a,v),a++,u=c.next();else{r??=new zf,o??=Lw(t,a,s,e);let b=e(a,v);if(Zv(t,r,a,b))t.updateValue(a,v),a++,s++,u=c.next();else if(!o.has(b))t.attach(a,t.create(a,v)),a++,s++,u=c.next();else{let A=e(a,m);r.set(A,t.detach(a)),s--}}}for(;!u.done;)Fw(t,r,e,t.length,u.value),u=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function Zv(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Fw(t,n,e,i,r){if(Zv(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Lw(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var zf=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function I(t,n,e,i,r,o,a,s){fi("NgControlFlow");let l=se(),c=nt(),u=Qn(c.consts,o);return Ms(l,c,t,n,e,i,r,u,256,a,s),ay}function ay(t,n,e,i,r,o,a,s){fi("NgControlFlow");let l=se(),c=nt(),u=Qn(c.consts,o);return Ms(l,c,t,n,e,i,r,u,512,a,s),ay}function T(t,n){fi("NgControlFlow");let e=se(),i=jr(),r=e[i]!==Ht?e[i]:-1,o=r!==-1?$f(e,Ne+r):void 0,a=0;if(kn(e,i,t)){let s=ce(null);try{if(o!==void 0&&U_(o,a),t!==-1){let l=Ne+t,c=$f(e,l),u=e_(e[ne],l),m=zE(c,u,e),v=ks(e,u,n,{dehydratedView:m});Os(c,v,a,Ma(u,m))}}finally{ce(s)}}else if(o!==void 0){let s=FE(o,a);s!==void 0&&(s[wt]=n)}}var Kv=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-pt}};function Qe(t,n){return n}var Xv=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Ze(t,n,e,i,r,o,a,s,l,c,u,m,v){fi("NgControlFlow");let _=se(),b=nt(),A=l!==void 0,R=se(),L=s?a.bind(R[zt][wt]):a,_e=new Xv(A,L);R[Ne+t]=_e,Ms(_,b,t+1,n,e,i,r,Qn(b.consts,o),256),A&&Ms(_,b,t+2,l,c,u,m,Qn(b.consts,v),512)}var Jv=class extends Qv{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-pt}at(n){return this.getLView(n)[wt].$implicit}attach(n,e){let i=e[mn];this.needsIndexUpdate||=n!==this.length,Os(this.lContainer,e,n,Ma(this.templateTNode,i)),ZF(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,KF(this.lContainer,n),XF(this.lContainer,n)}create(n,e){let i=yc(this.lContainer,this.templateTNode.tView.ssrId);return ks(this.hostLView,this.templateTNode,new Kv(this.lContainer,e,n),{dehydratedView:i})}destroy(n){nm(n[ne],n)}updateValue(n,e){this.getLView(n)[wt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[wt].$index=n}getLView(n){return JF(this.lContainer,n)}};function Ke(t){let n=ce(null),e=Li();try{let i=se(),r=i[ne],o=i[e],a=e+1,s=$f(i,a);if(o.liveCollection===void 0){let c=e_(r,a);o.liveCollection=new Jv(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(QF(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=jr(),u=l.length===0;if(kn(i,c,u)){let m=e+2,v=$f(i,m);if(u){let _=e_(r,m),b=zE(v,_,i),A=ks(i,_,void 0,{dehydratedView:b});Os(v,A,0,Ma(_,b))}else r.firstUpdatePass&&cm(v),U_(v,0)}}}finally{ce(n)}}function $f(t,n){return t[n]}function ZF(t,n){if(t.length<=pt)return;let e=pt+n,i=t[e],r=i?i[xo]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Gn];wN(o,r),Sa.delete(i[Vr]),r.detachedLeaveAnimationFns=void 0}}function KF(t,n){if(t.length<=pt)return;let e=pt+n,i=t[e],r=i?i[xo]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function XF(t,n){return _c(t,n)}function JF(t,n){return FE(t,n)}function e_(t,n){return gs(t,n)}function x(t,n,e){let i=se(),r=jr();if(kn(i,r,n)){let o=nt(),a=lc();xE(a,i,t,n,i[Ve],e)}return x}function t_(t,n,e,i,r){B_(n,t,e,r?"class":"style",i)}function f(t,n,e,i){let r=se(),o=r[ne],a=t+Ne,s=o.firstCreatePass?$_(a,r,2,n,F_,sf(),e,i):o.data[a];if(hr(s)){let l=r[Ni].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(eS(c),()=>(Vw(t,n,r,s,i),f))}}return Vw(t,n,r,s,i),f}function Vw(t,n,e,i,r){if(L_(i,e,t,n,sy),hs(i)){let o=e[ne];rm(o,e,i),g_(o,i,e)}r!=null&&Tc(e,i)}function p(){let t=nt(),n=Pt(),e=V_(n);return t.firstCreatePass&&G_(t,e),$g(e)&&Gg(),zg(),e.classesWithoutHost!=null&&K1(e)&&t_(t,e,se(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&X1(e)&&t_(t,e,se(),e.stylesWithoutHost,!1),p}function U(t,n,e,i){return f(t,n,e,i),p(),U}function xt(t,n,e,i){let r=se(),o=r[ne],a=t+Ne,s=o.firstCreatePass?jP(a,o,2,n,e,i):o.data[a];return L_(s,r,t,n,sy),i!=null&&Tc(r,s),xt}function Lt(){let t=Pt(),n=V_(t);return $g(n)&&Gg(),zg(),Lt}function en(t,n,e,i){return xt(t,n,e,i),Lt(),en}var sy=(t,n,e,i,r)=>(Br(!0),w_(n[Ve],i,mf()));function eL(t,n,e,i,r){let o=!Kf(n,e);if(Br(o),o)return w_(n[Ve],i,mf());let a=n[mn],s=sm(a,t,n,e);return zx(a,r)&&Zf(a,r,s.nextSibling),a&&(lx(e)||cx(s))&&hr(e)&&(OD(e),nE(s)),s}function IS(){sy=eL}function ee(t,n,e){let i=se(),r=i[ne],o=t+Ne,a=r.firstCreatePass?$_(o,i,8,"ng-container",F_,sf(),n,e):r.data[o];if(L_(a,i,t,"ng-container",TS),hs(a)){let s=i[ne];rm(s,i,a),g_(s,a,i)}return e!=null&&Tc(i,a),ee}function te(){let t=nt(),n=Pt(),e=V_(n);return t.firstCreatePass&&G_(t,e),te}function it(t,n,e){return ee(t,n,e),te(),it}var TS=(t,n,e,i,r)=>(Br(!0),eE(n[Ve],""));function tL(t,n,e,i,r){let o,a=!Kf(n,e);if(Br(a),a)return eE(n[Ve],"");let s=n[mn],l=sm(s,t,n,e),c=MO(s,r);return Zf(s,r,l),o=lm(c,l),o}function AS(){TS=tL}function ze(){return se()}function Et(t,n,e){let i=se(),r=jr();if(kn(i,r,n)){let o=nt(),a=lc();EE(a,i,t,n,i[Ve],e)}return Et}var dc=void 0;function nL(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var iL=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],dc,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],dc,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",dc,dc,dc],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",nL],pv={};function hn(t){let n=rL(t),e=jw(n);if(e)return e;let i=n.split("-")[0];if(e=jw(i),e)return e;if(i==="en")return iL;throw new P(701,!1)}function jw(t){return t in pv||(pv[t]=ai.ng&&ai.ng.common&&ai.ng.common.locales&&ai.ng.common.locales[t]),pv[t]}var gt=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(gt||{});function rL(t){return t.toLowerCase().replace(/_/g,"-")}var Oc="en-US",oL="USD";var aL=Oc;function RS(t){typeof t=="string"&&(aL=t.toLowerCase().replace(/_/g,"-"))}function F(t,n,e){let i=se(),r=nt(),o=Pt();return kS(r,i,i[Ve],o,t,n,e),F}function vm(t,n,e){let i=se(),r=nt(),o=Pt();return(o.type&3||e)&&XE(o,r,i,e,i[Ve],t,n,Mf(o,i,n)),vm}function kS(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=Mf(i,n,o),XE(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let m=0;m<u.length;m+=2){let v=u[m],_=u[m+1];l??=Mf(i,n,o),ww(i,n,v,_,r,l)}if(c&&c.length)for(let m of c)l??=Mf(i,n,o),ww(i,n,m,r,r,l)}}function y(t=1){return $D(t)}function sL(t,n){let e=null,i=lN(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?dE(t,o,!0):uN(i,o))return r}return e}function Me(t){let n=se()[zt][fn];if(!n.projection){let e=t?t.length:1,i=n.projection=gD(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?sL(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function Z(t,n=0,e,i,r,o){let a=se(),s=nt(),l=i?t+1:null;l!==null&&Ms(a,s,l,i,r,o,null,e);let c=Ns(s,Ne+t,16,null,e||null);c.projection===null&&(c.projection=n),Yg();let m=!a[mn]||lf();a[zt][fn].projection[c.projection]===null&&l!==null?lL(a,s,l):m&&!Sc(c)&&NN(s,a,c)}function lL(t,n,e){let i=Ne+e,r=n.data[i],o=t[i],a=yc(o,r.tView.ssrId),s=ks(t,r,void 0,{dehydratedView:a});Os(o,s,0,Ma(r,a))}function rt(t,n,e,i){return cS(t,n,e,i),rt}function dt(t,n,e){return lS(t,n,e),dt}function q(t){let n=se(),e=nt(),i=df();sc(i+1);let r=Z_(e,i);if(t.dirty&&SD(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=uS(n,i);t.reset(o,ax),t.notifyOnChanges()}return!0}return!1}function Y(){return Q_(se(),df())}function _m(t,n,e,i,r){return mS(n,cS(t,e,i,r)),_m}function ym(t,n,e,i){return mS(t,lS(n,e,i)),ym}function bm(t=1){sc(df()+t)}function $t(t){let n=PD();return rf(n,Ne+t)}function Cf(t,n){return t<<17|n<<2}function Ia(t){return t>>17&32767}function cL(t){return(t&2)==2}function dL(t,n){return t&131071|n<<17}function n_(t){return t|2}function Is(t){return(t&131068)>>2}function hv(t,n){return t&-131069|n<<2}function uL(t){return(t&1)===1}function i_(t){return t|1}function fL(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=Ia(a),l=Is(a);t[i]=e;let c=!1,u;if(Array.isArray(e)){let m=e;u=m[1],(u===null||us(m,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let v=Ia(t[s+1]);t[i+1]=Cf(v,s),v!==0&&(t[v+1]=hv(t[v+1],i)),t[s+1]=dL(t[s+1],i)}else t[i+1]=Cf(s,0),s!==0&&(t[s+1]=hv(t[s+1],i)),s=i;else t[i+1]=Cf(l,0),s===0?s=i:t[l+1]=hv(t[l+1],i),l=i;c&&(t[i+1]=n_(t[i+1])),Bw(t,u,i,!0),Bw(t,u,i,!1),mL(n,u,t,i,o),a=Cf(s,l),o?n.classBindings=a:n.styleBindings=a}function mL(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&us(o,n)>=0&&(e[i+1]=i_(e[i+1]))}function Bw(t,n,e,i){let r=t[e+1],o=n===null,a=i?Ia(r):Is(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];pL(l,n)&&(s=!0,t[a+1]=i?i_(c):n_(c)),a=i?Ia(c):Is(c)}s&&(t[e+1]=i?n_(r):i_(r))}function pL(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?us(t,n)>=0:!1}var Bi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function hL(t){return t.substring(Bi.key,Bi.keyEnd)}function gL(t){return vL(t),OS(t,NS(t,0,Bi.textEnd))}function OS(t,n){let e=Bi.textEnd;return e===n?-1:(n=Bi.keyEnd=_L(t,Bi.key=n,e),NS(t,n,e))}function vL(t){Bi.key=0,Bi.keyEnd=0,Bi.value=0,Bi.valueEnd=0,Bi.textEnd=t.length}function NS(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function _L(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function E(t,n,e){return PS(t,n,e,!1),E}function K(t,n){return PS(t,n,null,!0),K}function Gt(t){bL(SL,yL,t,!0)}function yL(t,n){for(let e=gL(n);e>=0;e=OS(n,e))Ju(t,hL(n),!0)}function PS(t,n,e,i){let r=se(),o=nt(),a=ac(2);if(o.firstUpdatePass&&LS(o,t,a,i),n!==Ht&&kn(r,a,n)){let s=o.data[Li()];VS(o,s,r,r[Ve],t,r[a+1]=IL(n,e),i,a)}}function bL(t,n,e,i){let r=nt(),o=ac(2);r.firstUpdatePass&&LS(r,null,o,i);let a=se();if(e!==Ht&&kn(a,o,e)){let s=r.data[Li()];if(jS(s,i)&&!FS(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=Yu(l,e||"")),t_(r,s,a,e,i)}else ML(r,s,a,a[Ve],a[o+1],a[o+1]=EL(t,n,e),i,o)}}function FS(t,n){return n>=t.expandoStartIndex}function LS(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Li()],a=FS(t,e);jS(o,i)&&n===null&&!a&&(n=!1),n=CL(r,o,n,i),fL(r,o,n,e,a,i)}}function CL(t,n,e,i){let r=BD(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=gv(null,t,n,e,i),e=Cc(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=gv(r,t,n,e,i),o===null){let l=DL(t,n,i);l!==void 0&&Array.isArray(l)&&(l=gv(null,t,n,l[1],i),l=Cc(l,n.attrs,i),wL(t,n,i,l))}else o=xL(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function DL(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Is(i)!==0)return t[Ia(i)]}function wL(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Ia(r)]=i}function xL(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Cc(i,a,e)}return Cc(i,n.attrs,e)}function gv(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Cc(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Cc(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Ju(t,a,e?!0:n[++o]))}return t===void 0?null:t}function EL(t,n,e){if(e==null||e==="")return Xt;let i=[],r=Ui(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function SL(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Ju(t,i,e)}function ML(t,n,e,i,r,o,a,s){r===Ht&&(r=Xt);let l=0,c=0,u=0<r.length?r[0]:null,m=0<o.length?o[0]:null;for(;u!==null||m!==null;){let v=l<r.length?r[l+1]:void 0,_=c<o.length?o[c+1]:void 0,b=null,A;u===m?(l+=2,c+=2,v!==_&&(b=m,A=_)):m===null||u!==null&&u<m?(l+=2,b=u):(c+=2,b=m,A=_),b!==null&&VS(t,n,e,i,b,A,a,s),u=l<r.length?r[l]:null,m=c<o.length?o[c]:null}}function VS(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],u=uL(c)?Hw(l,n,e,r,Is(c),a):void 0;if(!Gf(u)){Gf(o)||cL(c)&&(o=Hw(l,null,e,r,s,a));let m=Fg(Li(),e);FN(i,a,m,r,o)}}function Hw(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,m=u===null,v=e[r+1];v===Ht&&(v=m?Xt:void 0);let _=m?ef(v,i):u===i?v:void 0;if(c&&!Gf(_)&&(_=ef(l,i)),Gf(_)&&(s=_,a))return s;let b=t[r+1];r=a?Ia(b):Is(b)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=ef(l,i))}return s}function Gf(t){return t!==void 0}function IL(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Jl(Ui(t)))),t}function jS(t,n){return(t.flags&(n?8:16))!==0}function g(t,n=""){let e=se(),i=nt(),r=t+Ne,o=i.firstCreatePass?Ns(i,r,1,n,null):i.data[r],a=BS(i,e,o,n);e[r]=a,pf()&&N_(i,e,a,o),vs(o,!1)}var BS=(t,n,e,i)=>(Br(!0),Jx(n[Ve],i));function TL(t,n,e,i){let r=!Kf(n,e);if(Br(r),r)return Jx(n[Ve],i);let o=n[mn];return sm(o,t,n,e)}function HS(){BS=TL}function US(t,n,e,i=""){return kn(t,jr(),e)?n+dr(e)+i:Ht}function AL(t,n,e,i,r,o=""){let a=Zg(),s=q_(t,a,e,r);return ac(2),s?n+dr(e)+i+dr(r)+o:Ht}function RL(t,n,e,i,r,o,a,s=""){let l=Zg(),c=KE(t,l,e,r,a);return ac(3),c?n+dr(e)+i+dr(r)+o+dr(a)+s:Ht}function ve(t){return z("",t),ve}function z(t,n,e){let i=se(),r=US(i,t,n,e);return r!==Ht&&ly(i,Li(),r),z}function Dr(t,n,e,i,r){let o=se(),a=AL(o,t,n,e,i,r);return a!==Ht&&ly(o,Li(),a),Dr}function wr(t,n,e,i,r,o,a){let s=se(),l=RL(s,t,n,e,i,r,o,a);return l!==Ht&&ly(s,Li(),l),wr}function ly(t,n,e){let i=Fg(n,t);XO(t[Ve],i,e)}function Cm(t,n,e){ty(n)&&(n=n());let i=se(),r=jr();if(kn(i,r,n)){let o=nt(),a=lc();xE(a,i,t,n,i[Ve],e)}return Cm}function cy(t,n){let e=ty(t);return e&&t.set(n),e}function Dm(t,n){let e=se(),i=nt(),r=Pt();return kS(i,e,e[Ve],r,t,n),Dm}function Xn(t){return kn(se(),jr(),t)?dr(t):Ht}function dy(t,n,e=""){return US(se(),t,n,e)}function Uw(t,n,e){let i=nt();i.firstCreatePass&&zS(n,i.data,i.blueprint,Fi(t),e)}function zS(t,n,e,i,r){if(t=Nt(t),Array.isArray(t))for(let o=0;o<t.length;o++)zS(t[o],n,e,i,r);else{let o=nt(),a=se(),s=Pt(),l=pa(t)?t:Nt(t.provide),c=Rg(t),u=s.providerIndexes&1048575,m=s.directiveStart,v=s.providerIndexes>>20;if(pa(t)||!t.multi){let _=new Ea(c,r,J,null),b=_v(l,n,r?u:u+v,m);b===-1?(bv(Of(s,a),o,l),vv(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(_),a.push(_)):(e[b]=_,a[b]=_)}else{let _=_v(l,n,u+v,m),b=_v(l,n,u,u+v),A=_>=0&&e[_],R=b>=0&&e[b];if(r&&!R||!r&&!A){bv(Of(s,a),o,l);let L=NL(r?OL:kL,e.length,r,i,c,t);!r&&R&&(e[b].providerFactory=L),vv(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(L),a.push(L)}else{let L=$S(e[r?b:_],c,!r&&i);vv(o,t,_>-1?_:b,L)}!r&&i&&R&&e[b].componentProviders++}}}function vv(t,n,e,i){let r=pa(n),o=DD(n);if(r||o){let l=(o?Nt(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function $S(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function _v(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function kL(t,n,e,i,r){return r_(this.multi,[])}function OL(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=pc(i,i[ne],this.providerFactory.index,r);a=l.slice(0,s),r_(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],r_(o,a);return a}function r_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function NL(t,n,e,i,r,o){let a=new Ea(t,e,J,null);return a.multi=[],a.index=n,a.componentProviders=0,$S(a,r,i&&!e),a}function Ce(t,n){return e=>{e.providersResolver=(i,r)=>Uw(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Uw(i,r?r(n):n,!0))}}function ot(t,n){let e=_s()+t,i=se();return i[e]===Ht?mm(i,e,n()):BP(i,e)}function Vs(t,n,e){return GS(se(),_s(),t,n,e)}function uy(t,n,e,i,r){return FL(se(),_s(),t,n,e,i,r)}function fy(t,n){let e=t[n];return e===Ht?void 0:e}function GS(t,n,e,i,r,o){let a=n+e;return kn(t,a,r)?mm(t,a+1,o?i.call(o,r):i(r)):fy(t,a+1)}function PL(t,n,e,i,r,o,a){let s=n+e;return q_(t,s,r,o)?mm(t,s+2,a?i.call(a,r,o):i(r,o)):fy(t,s+2)}function FL(t,n,e,i,r,o,a,s){let l=n+e;return KE(t,l,r,o,a)?mm(t,l+3,s?i.call(s,r,o,a):i(r,o,a)):fy(t,l+3)}function Wt(t,n){let e=nt(),i,r=t+Ne;e.firstCreatePass?(i=LL(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=bo(i.type,!0)),a,s=ln(J);try{let l=kf(!1),c=o();return kf(l),Lg(e,se(),r,c),c}finally{ln(s)}}function LL(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function mi(t,n,e){let i=t+Ne,r=se(),o=rf(r,i);return WS(r,i)?GS(r,_s(),n,o.transform,e,o):o.transform(e)}function js(t,n,e,i){let r=t+Ne,o=se(),a=rf(o,r);return WS(o,r)?PL(o,_s(),n,a.transform,e,i,a):a.transform(e,i)}function WS(t,n){return t[ne].data[n].pure}function Bs(t,n){return am(t,n)}var Wf=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},my=(()=>{class t{compileModuleSync(e){return new Uf(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=wg(e),o=lE(r.declarations).reduce((a,s)=>{let l=ki(s);return l&&a.push(new Io(l)),a},[]);return new Wf(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var qS=(()=>{class t{applicationErrorHandler=d(Zn);appRef=d(At);taskService=d(vr);ngZone=d(ie);zonelessEnabled=d(cc);tracing=d(zi,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new me;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Kl):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(rv,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?YD:ev;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Kl+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function YS(){return[{provide:Ri,useExisting:qS},{provide:ie,useClass:Xl},{provide:cc,useValue:!0}]}function VL(){return typeof $localize<"u"&&$localize.locale||Oc}var Hs=new C("",{factory:()=>d(Hs,{optional:!0,skipSelf:!0})||VL()}),py=new C("",{factory:()=>oL});function He(t){return oD(t)}function Rt(t,n){return Nl(t,n?.equal)}var jL=t=>t;function wm(t,n){if(typeof t=="function"){let e=ig(t,jL,n?.equal);return QS(e,n?.debugName)}else{let e=ig(t.source,t.computation,t.equal);return QS(e,t.debugName)}}function QS(t,n){let e=t[bt],i=t;return i.set=r=>iD(e,r),i.update=r=>rD(e,r),i.asReadonly=hf.bind(t),i}var yy={JSACTION:"__jsaction",OWNER:"__owner"},JS={};function BL(t){return t[yy.JSACTION]}function ZS(t,n){t[yy.JSACTION]=n}function HL(t){return JS[t]}function UL(t,n){JS[t]=n}var ue={CLICK:"click",CLICKMOD:"clickmod",DBLCLICK:"dblclick",FOCUS:"focus",FOCUSIN:"focusin",BLUR:"blur",FOCUSOUT:"focusout",SUBMIT:"submit",KEYDOWN:"keydown",KEYPRESS:"keypress",KEYUP:"keyup",MOUSEOVER:"mouseover",MOUSEOUT:"mouseout",MOUSEENTER:"mouseenter",MOUSELEAVE:"mouseleave",POINTEROVER:"pointerover",POINTEROUT:"pointerout",POINTERENTER:"pointerenter",POINTERLEAVE:"pointerleave",ERROR:"error",LOAD:"load",TOUCHSTART:"touchstart",TOUCHEND:"touchend",TOUCHMOVE:"touchmove",TOGGLE:"toggle"},zL=[ue.MOUSEENTER,ue.MOUSELEAVE,"pointerenter","pointerleave"],OK=[ue.CLICK,ue.DBLCLICK,ue.FOCUSIN,ue.FOCUSOUT,ue.KEYDOWN,ue.KEYUP,ue.KEYPRESS,ue.MOUSEOVER,ue.MOUSEOUT,ue.SUBMIT,ue.TOUCHSTART,ue.TOUCHEND,ue.TOUCHMOVE,"touchcancel","auxclick","change","compositionstart","compositionupdate","compositionend","beforeinput","input","select","copy","cut","paste","mousedown","mouseup","wheel","contextmenu","dragover","dragenter","dragleave","drop","dragstart","dragend","pointerdown","pointermove","pointerup","pointercancel","pointerover","pointerout","gotpointercapture","lostpointercapture","ended","loadedmetadata","pagehide","pageshow","visibilitychange","beforematch"],$L=[ue.FOCUS,ue.BLUR,ue.ERROR,ue.LOAD,ue.TOGGLE],by=t=>$L.indexOf(t)>=0;function GL(t){return t===ue.MOUSEENTER?ue.MOUSEOVER:t===ue.MOUSELEAVE?ue.MOUSEOUT:t===ue.POINTERENTER?ue.POINTEROVER:t===ue.POINTERLEAVE?ue.POINTEROUT:t}function WL(t,n,e,i){let r=!1;by(n)&&(r=!0);let o=typeof i=="boolean"?{capture:r,passive:i}:r;return t.addEventListener(n,e,o),{eventType:n,handler:e,capture:r,passive:i}}function qL(t,n){if(t.removeEventListener){let e=typeof n.passive=="boolean"?{capture:n.capture}:n.capture;t.removeEventListener(n.eventType,n.handler,e)}else t.detachEvent&&t.detachEvent(`on${n.eventType}`,n.handler)}function YL(t){t.preventDefault?t.preventDefault():t.returnValue=!1}var KS=typeof navigator<"u"&&/Macintosh/.test(navigator.userAgent);function QL(t){return t.which===2||t.which==null&&t.button===4}function ZL(t){return KS&&t.metaKey||!KS&&t.ctrlKey||QL(t)||t.shiftKey}function KL(t,n,e){let i=t.relatedTarget;return(t.type===ue.MOUSEOVER&&n===ue.MOUSEENTER||t.type===ue.MOUSEOUT&&n===ue.MOUSELEAVE||t.type===ue.POINTEROVER&&n===ue.POINTERENTER||t.type===ue.POINTEROUT&&n===ue.POINTERLEAVE)&&(!i||i!==e&&!e.contains(i))}function XL(t,n){let e={};for(let i in t){if(i==="srcElement"||i==="target")continue;let r=i,o=t[r];typeof o!="function"&&(e[r]=o)}return t.type===ue.MOUSEOVER?e.type=ue.MOUSEENTER:t.type===ue.MOUSEOUT?e.type=ue.MOUSELEAVE:t.type===ue.POINTEROVER?e.type=ue.POINTERENTER:e.type=ue.POINTERLEAVE,e.target=e.srcElement=n,e.bubbles=!1,e._originalEvent=t,e}var Mm=class{element;handlerInfos=[];constructor(n){this.element=n}addEventListener(n,e,i){this.handlerInfos.push(WL(this.element,n,e(this.element),i))}cleanUp(){for(let n=0;n<this.handlerInfos.length;n++)qL(this.element,this.handlerInfos[n]);this.handlerInfos=[]}},JL={EVENT_ACTION_SEPARATOR:":"};function Ro(t){return t.eventType}function Cy(t,n){t.eventType=n}function Em(t){return t.event}function eM(t,n){t.event=n}function tM(t){return t.targetElement}function nM(t,n){t.targetElement=n}function iM(t){return t.eic}function e2(t,n){t.eic=n}function t2(t){return t.timeStamp}function n2(t,n){t.timeStamp=n}function Sm(t){return t.eia}function rM(t,n,e){t.eia=[n,e]}function hy(t){t.eia=void 0}function xm(t){return t[1]}function i2(t){return t.eirp}function oM(t,n){t.eirp=n}function aM(t){return t.eir}function sM(t,n){t.eir=n}function lM(t){return{eventType:t.eventType,event:t.event,targetElement:t.targetElement,eic:t.eic,eia:t.eia,timeStamp:t.timeStamp,eirp:t.eirp,eiack:t.eiack,eir:t.eir}}function r2(t,n,e,i,r,o,a,s){return{eventType:t,event:n,targetElement:e,eic:i,timeStamp:r,eia:o,eirp:a,eiack:s}}var gy=class t{eventInfo;constructor(n){this.eventInfo=n}getEventType(){return Ro(this.eventInfo)}setEventType(n){Cy(this.eventInfo,n)}getEvent(){return Em(this.eventInfo)}setEvent(n){eM(this.eventInfo,n)}getTargetElement(){return tM(this.eventInfo)}setTargetElement(n){nM(this.eventInfo,n)}getContainer(){return iM(this.eventInfo)}setContainer(n){e2(this.eventInfo,n)}getTimestamp(){return t2(this.eventInfo)}setTimestamp(n){n2(this.eventInfo,n)}getAction(){let n=Sm(this.eventInfo);if(n)return{name:n[0],element:n[1]}}setAction(n){if(!n){hy(this.eventInfo);return}rM(this.eventInfo,n.name,n.element)}getIsReplay(){return i2(this.eventInfo)}setIsReplay(n){oM(this.eventInfo,n)}getResolved(){return aM(this.eventInfo)}setResolved(n){sM(this.eventInfo,n)}clone(){return new t(lM(this.eventInfo))}},o2={},a2=/\s*;\s*/,s2=ue.CLICK,vy=class{a11yClickSupport=!1;clickModSupport=!0;syntheticMouseEventSupport;updateEventInfoForA11yClick=void 0;preventDefaultForA11yClick=void 0;populateClickOnlyAction=void 0;constructor({syntheticMouseEventSupport:n=!1,clickModSupport:e=!0}={}){this.syntheticMouseEventSupport=n,this.clickModSupport=e}resolveEventType(n){this.clickModSupport&&Ro(n)===ue.CLICK&&ZL(Em(n))?Cy(n,ue.CLICKMOD):this.a11yClickSupport&&this.updateEventInfoForA11yClick(n)}resolveAction(n){aM(n)||(this.populateAction(n,tM(n)),sM(n,!0))}resolveParentAction(n){let e=Sm(n),i=e&&xm(e);hy(n);let r=i&&this.getParentNode(i);r&&this.populateAction(n,r)}populateAction(n,e){let i=e;for(;i&&i!==iM(n)&&(i.nodeType===Node.ELEMENT_NODE&&this.populateActionOnElement(i,n),!Sm(n));)i=this.getParentNode(i);let r=Sm(n);if(r&&(this.a11yClickSupport&&this.preventDefaultForA11yClick(n),this.syntheticMouseEventSupport&&(Ro(n)===ue.MOUSEENTER||Ro(n)===ue.MOUSELEAVE||Ro(n)===ue.POINTERENTER||Ro(n)===ue.POINTERLEAVE)))if(KL(Em(n),Ro(n),xm(r))){let o=XL(Em(n),xm(r));eM(n,o),nM(n,xm(r))}else hy(n)}getParentNode(n){let e=n[yy.OWNER];if(e)return e;let i=n.parentNode;return i?.nodeName==="#document-fragment"?i?.host??null:i}populateActionOnElement(n,e){let i=this.parseActions(n),r=i[Ro(e)];r!==void 0&&rM(e,r,n),this.a11yClickSupport&&this.populateClickOnlyAction(n,e,i)}parseActions(n){let e=BL(n);if(!e){let i=n.getAttribute(vf.JSACTION);if(!i)e=o2,ZS(n,e);else{if(e=HL(i),!e){e={};let r=i.split(a2);for(let o=0;o<r.length;o++){let a=r[o];if(!a)continue;let s=a.indexOf(JL.EVENT_ACTION_SEPARATOR),l=s!==-1,c=l?a.substr(0,s).trim():s2,u=l?a.substr(s+1).trim():a;e[c]=u}UL(i,e)}ZS(n,e)}}return e}addA11yClickSupport(n,e,i){this.a11yClickSupport=!0,this.updateEventInfoForA11yClick=n,this.preventDefaultForA11yClick=e,this.populateClickOnlyAction=i}},cM=(function(t){return t[t.I_AM_THE_JSACTION_FRAMEWORK=0]="I_AM_THE_JSACTION_FRAMEWORK",t})(cM||{}),_y=class{dispatchDelegate;actionResolver;eventReplayer;eventReplayScheduled=!1;replayEventInfoWrappers=[];constructor(n,{actionResolver:e,eventReplayer:i}={}){this.dispatchDelegate=n,this.actionResolver=e,this.eventReplayer=i}dispatch(n){let e=new gy(n);this.actionResolver?.resolveEventType(n),this.actionResolver?.resolveAction(n);let i=e.getAction();if(i&&l2(i.element,e)&&YL(e.getEvent()),this.eventReplayer&&e.getIsReplay()){this.scheduleEventInfoWrapperReplay(e);return}this.dispatchDelegate(e)}scheduleEventInfoWrapperReplay(n){this.replayEventInfoWrappers.push(n),!this.eventReplayScheduled&&(this.eventReplayScheduled=!0,Promise.resolve().then(()=>{this.eventReplayScheduled=!1,this.eventReplayer(this.replayEventInfoWrappers)}))}};function l2(t,n){return t.tagName==="A"&&(n.getEventType()===ue.CLICK||n.getEventType()===ue.CLICKMOD)}var dM=Symbol.for("propagationStopped"),Dy={REPLAY:101};var c2="`preventDefault` called during event replay.";var d2="`composedPath` called during event replay.",Im=class{dispatchDelegate;clickModSupport;actionResolver;dispatcher;constructor(n,e=!0){this.dispatchDelegate=n,this.clickModSupport=e,this.actionResolver=new vy({clickModSupport:e}),this.dispatcher=new _y(i=>{this.dispatchToDelegate(i)},{actionResolver:this.actionResolver})}dispatch(n){this.dispatcher.dispatch(n)}dispatchToDelegate(n){for(n.getIsReplay()&&m2(n),u2(n);n.getAction();){if(p2(n),by(n.getEventType())&&n.getAction().element!==n.getTargetElement()||(this.dispatchDelegate(n.getEvent(),n.getAction().name),f2(n)))return;this.actionResolver.resolveParentAction(n.eventInfo)}}};function u2(t){let n=t.getEvent(),e=t.getEvent().stopPropagation.bind(n),i=()=>{n[dM]=!0,e()};Aa(n,"stopPropagation",i),Aa(n,"stopImmediatePropagation",i)}function f2(t){return!!t.getEvent()[dM]}function m2(t){let n=t.getEvent(),e=t.getTargetElement(),i=n.preventDefault.bind(n);Aa(n,"target",e),Aa(n,"eventPhase",Dy.REPLAY),Aa(n,"preventDefault",()=>{throw i(),new Error(c2+"")}),Aa(n,"composedPath",()=>{throw new Error(d2+"")})}function p2(t){let n=t.getEvent(),e=t.getAction()?.element;e&&Aa(n,"currentTarget",e,{configurable:!0})}function Aa(t,n,e,{configurable:i=!1}={}){Object.defineProperty(t,n,{value:e,configurable:i})}function uM(t,n){t.ecrd(e=>{n.dispatch(e)},cM.I_AM_THE_JSACTION_FRAMEWORK)}function h2(t){return t?.q??[]}function g2(t){t&&(XS(t.c,t.et,t.h),XS(t.c,t.etc,t.h,!0))}function XS(t,n,e,i){for(let r=0;r<n.length;r++)t.removeEventListener(n[r],e,i)}var v2=!1,fM=(()=>{class t{static MOUSE_SPECIAL_SUPPORT=v2;containerManager;eventHandlers={};browserEventTypeToExtraEventTypes={};dispatcher=null;queuedEventInfos=[];constructor(e){this.containerManager=e}handleEvent(e,i,r){let o=r2(e,i,i.target,r,Date.now());this.handleEventInfo(o)}handleEventInfo(e){if(!this.dispatcher){oM(e,!0),this.queuedEventInfos?.push(e);return}this.dispatcher(e)}addEvent(e,i,r){if(e in this.eventHandlers||!this.containerManager||!t.MOUSE_SPECIAL_SUPPORT&&zL.indexOf(e)>=0)return;let o=(s,l,c)=>{this.handleEvent(s,l,c)};this.eventHandlers[e]=o;let a=GL(i||e);if(a!==e){let s=this.browserEventTypeToExtraEventTypes[a]||[];s.push(e),this.browserEventTypeToExtraEventTypes[a]=s}this.containerManager.addEventListener(a,s=>l=>{o(e,l,s)},r)}replayEarlyEvents(e=window._ejsa){e&&(this.replayEarlyEventInfos(e.q),g2(e),delete window._ejsa)}replayEarlyEventInfos(e){for(let i=0;i<e.length;i++){let r=e[i],o=this.getEventTypesForBrowserEventType(r.eventType);for(let a=0;a<o.length;a++){let s=lM(r);Cy(s,o[a]),this.handleEventInfo(s)}}}getEventTypesForBrowserEventType(e){let i=[];return this.eventHandlers[e]&&i.push(e),this.browserEventTypeToExtraEventTypes[e]&&i.push(...this.browserEventTypeToExtraEventTypes[e]),i}handler(e){return this.eventHandlers[e]}cleanUp(){this.containerManager?.cleanUp(),this.containerManager=null,this.eventHandlers={},this.browserEventTypeToExtraEventTypes={},this.dispatcher=null,this.queuedEventInfos=[]}registerDispatcher(e,i){this.ecrd(e,i)}ecrd(e,i){if(this.dispatcher=e,this.queuedEventInfos?.length){for(let r=0;r<this.queuedEventInfos.length;r++)this.handleEventInfo(this.queuedEventInfos[r]);this.queuedEventInfos=null}}}return t})();function mM(t,n=window){return h2(n._ejsas?.[t])}function wy(t,n=window){n._ejsas&&(n._ejsas[t]=void 0)}var xM=Symbol("InputSignalNode#UNSET"),k2=fe(D({},Pl),{transformFn:void 0,applyValueToInputSignal(t,n){ia(t,n)}});function EM(t,n){let e=Object.create(k2);e.value=t,e.transformFn=n?.transform;function i(){if(uo(e),e.value===xM){let r=null;throw new P(-950,r)}return e.value}return i[bt]=e,i}var On=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>wc(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function pM(t,n){return EM(t,n)}function O2(t){return EM(xM,t)}var km=(pM.required=O2,pM);function hM(t,n){return K_(n)}function N2(t,n){return X_(n)}var Pc=(hM.required=N2,hM);function gM(t,n){return K_(n)}function P2(t,n){return X_(n)}var SM=(gM.required=P2,gM);var Ey=new C(""),F2=new C("");function Nc(t){return!t.moduleRef}function L2(t){let n=Nc(t)?t.r3Injector:t.moduleRef.injector,e=n.get(ie);return e.run(()=>{Nc(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Zn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Nc(t)){let o=()=>n.destroy(),a=t.platformInjector.get(Ey);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(Ey);a.add(o),t.moduleRef.onDestroy(()=>{mc(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return j2(i,e,()=>{let o=n.get(vr),a=o.add(),s=n.get(ry);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(Hs,Oc);if(RS(l||Oc),!n.get(F2,!0))return Nc(t)?n.get(At):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Nc(t)){let u=n.get(At);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return V2?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var V2;function j2(t,n,e){try{let i=e();return To(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Am=null;function B2(t=[],n){return oe.create({name:n,providers:[{provide:ic,useValue:"platform"},{provide:Ey,useValue:new Set([()=>Am=null])},...t]})}function H2(t=[]){if(Am)return Am;let n=B2(t);return Am=n,xS(),U2(n),n}function U2(t){let n=t.get(qf,null);Bt(t,()=>{n?.forEach(e=>e())})}var Tm=new WeakSet,vM="";function _M(t){return t.get(u_,Sx)}function MM(){let t=[{provide:u_,useFactory:()=>{let n=!0;{let e=d(ui);n=!!window._ejsas?.[e]}return n&&fi("NgEventReplay"),n}}];return t.push({provide:ur,useValue:()=>{let n=d(At),{injector:e}=n;if(!Tm.has(n)){let i=d(f_);if(_M(e)){Px();let r=e.get(ui),o=Ox(r,(a,s,l)=>{a.nodeType===Node.ELEMENT_NODE&&(Tx(a,s,l),Ax(a,i))});n.onDestroy(o)}}},multi:!0},{provide:Ao,useFactory:()=>{let n=d(At),{injector:e}=n;return()=>{if(!_M(e)||Tm.has(n))return;Tm.add(n);let i=e.get(ui);n.onDestroy(()=>{Tm.delete(n),wy(i)}),n.whenStable().then(()=>{if(n.destroyed)return;let r=e.get(kx);z2(r,e);let o=e.get(f_);o.get(vM)?.forEach(Rx),o.delete(vM);let a=r.instance;Ux(e)?n.onDestroy(()=>a.cleanUp()):a.cleanUp()})}},multi:!0}),t}var z2=(t,n)=>{let e=n.get(ui),i=window._ejsas[e],r=t.instance=new fM(new Mm(i.c));for(let s of i.et)r.addEvent(s);for(let s of i.etc)r.addEvent(s);let o=mM(e);r.replayEarlyEventInfos(o),wy(e);let a=new Im(s=>{$2(n,s,s.currentTarget)});uM(r,a)};function $2(t,n,e){let i=(e&&e.getAttribute(Ec))??"";/d\d+/.test(i)?G2(i,t,n,e):n.eventPhase===Dy.REPLAY&&m_(n,e)}function G2(t,n,e,i){let r=n.get(Mx);r.push({event:e,currentTarget:i}),MS(n,t,W2(r))}function W2(t){return n=>{let e=new Set(n),i=[];for(let{event:r,currentTarget:o}of t){let a=o.getAttribute(Ec);e.has(a)?m_(r,o):i.push({event:r,currentTarget:o})}t.length=0,t.push(...i)}}var yM=!1;var q2=1e4;function Y2(){yM||(yM=!0,jx(),IS(),HS(),AS(),bS(),oS(),UE(),wE())}function Q2(t){return t.whenStable()}function IM(){let t=[{provide:Rs,useFactory:()=>{let n=!0;return n=!!d(As,{optional:!0})?.get(p_,null),n&&fi("NgHydration"),n}},{provide:ur,useValue:()=>{VE(!1);let n=d(V);d(Rs)&&(Gx(n),Y2())},multi:!0}];return t.push({provide:d_,useFactory:()=>d(Rs)},{provide:Ao,useFactory:()=>{let n=d(Ri);if(d(Rs)){let e=d(At);return()=>{Q2(e).then(()=>{e.destroyed||(z_(e),n.notify(7))})}}return()=>{}},multi:!0}),fr(t)}var ane=q2-1e3;var Le=(()=>{class t{static __NG_ELEMENT_ID__=Z2}return t})();function Z2(t){return K2(Pt(),se(),(t&16)===16)}function K2(t,n,e){if(hr(t)&&!e){let i=di(t.index,n);return new Mo(i,i)}else if(t.type&175){let i=n[zt];return new Mo(i,n)}return null}var Sy=class{supports(n){return W_(n)}create(n){return new My(n)}},X2=(t,n)=>n,My=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||X2}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let a=!i||e&&e.currentIndex<bM(i,r,o)?e:i,s=bM(a,r,o),l=a.currentIndex;if(a===i)r--,i=i._nextRemoved;else if(e=e._next,a.previousIndex==null)r++;else{o||(o=[]);let c=s-r,u=l-r;if(c!=u){for(let v=0;v<c;v++){let _=v<o.length?o[v]:o[v]=0,b=_+v;u<=b&&b<c&&(o[v]=_+1)}let m=a.previousIndex;o[m]=u-c}}s!==l&&n(a,s,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!W_(n))throw new P(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,a;if(Array.isArray(n)){this.length=n.length;for(let s=0;s<this.length;s++)o=n[s],a=this._trackByFn(s,o),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,o,a,s),i=!0):(i&&(e=this._verifyReinsertion(e,o,a,s)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,QE(n,s=>{a=this._trackByFn(r,s),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,s,a,r),i=!0):(i&&(e=this._verifyReinsertion(e,s,a,r)),Object.is(e.item,s)||this._addIdentityChange(e,s)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new Iy(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Rm),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Rm),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},Iy=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},Ty=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Rm=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new Ty,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function bM(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function CM(){return new Gr([new Sy])}var Gr=(()=>{class t{factories;static \u0275prov=w({token:t,providedIn:"root",factory:CM});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||CM())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new P(901,!1)}}return t})();function TM(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Be(Oe.BootstrapApplicationStart);try{let o=r?.injector??H2(i),a=[YS(),ZD,...e||[]],s=new bc({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return L2({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Be(Oe.BootstrapApplicationEnd)}}function W(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function $i(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var xy=Symbol("NOT_SET"),AM=new Set,J2=fe(D({},Pl),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:xy,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==xy&&!es(this))return this.signal;try{for(let r of this.cleanup??AM)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Nr(this),i;try{i=this.userFn.apply(null,n)}finally{fo(this,e)}return(this.value===xy||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Ay=class extends gc{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(pn),a),this.scheduler=r;for(let s of R_){let l=e[s];if(l===void 0)continue;let c=Object.create(J2);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(uo(c),c.value),c.signal[bt]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[s]=c,this.hooks[s]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??AM)e()}finally{mo(n)}}};function RM(t,n){let e=n?.injector??d(oe),i=e.get(Ri),r=e.get(tm),o=e.get(zi,null,{optional:!0});r.impl??=e.get(k_);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(ys,null,{optional:!0}),l=new Ay(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Om(t,n){let e=ki(t),i=n.elementInjector||fs();return new Io(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}function kM(t){let n=ki(t);if(!n)return null;let e=new Io(n);return{get selector(){return e.selector},get type(){return e.componentType},get inputs(){return e.inputs},get outputs(){return e.outputs},get ngContentSelectors(){return e.ngContentSelectors},get isStandalone(){return n.standalone},get isSignal(){return n.signals}}}var OM=null;function pi(){return OM}function Ry(t){OM??=t}var Fc=class{},Nm=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(NM),providedIn:"platform"})}return t})();var NM=(()=>{class t extends Nm{_location;_history;_doc=d(V);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return pi().getBaseHref(this._doc)}onPopState(e){let i=pi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=pi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function LM(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function PM(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Oo(t){return t&&t[0]!=="?"?`?${t}`:t}var Us=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(tV),providedIn:"root"})}return t})(),eV=new C(""),tV=(()=>{class t extends Us{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(V).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return LM(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Oo(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+Oo(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+Oo(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(le(Nm),le(eV,8))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var No=(()=>{class t{_subject=new k;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=rV(PM(FM(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Oo(i))}normalize(e){return t.stripTrailingSlash(iV(this._basePath,FM(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Oo(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Oo(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Oo;static joinWithSlash=LM;static stripTrailingSlash=PM;static \u0275fac=function(i){return new(i||t)(le(Us))};static \u0275prov=w({token:t,factory:()=>nV(),providedIn:"root"})}return t})();function nV(){return new No(le(Us))}function iV(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function FM(t){return t.replace(/\/index.html$/,"")}function rV(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var HM={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},Ly=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(Ly||{});var tn=(function(t){return t[t.Format=0]="Format",t[t.Standalone=1]="Standalone",t})(tn||{}),Xe=(function(t){return t[t.Narrow=0]="Narrow",t[t.Abbreviated=1]="Abbreviated",t[t.Wide=2]="Wide",t[t.Short=3]="Short",t})(Xe||{}),Nn=(function(t){return t[t.Short=0]="Short",t[t.Medium=1]="Medium",t[t.Long=2]="Long",t[t.Full=3]="Full",t})(Nn||{}),Pn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function UM(t){return hn(t)[gt.LocaleId]}function zM(t,n,e){let i=hn(t),r=[i[gt.DayPeriodsFormat],i[gt.DayPeriodsStandalone]],o=hi(r,n);return hi(o,e)}function $M(t,n,e){let i=hn(t),r=[i[gt.DaysFormat],i[gt.DaysStandalone]],o=hi(r,n);return hi(o,e)}function GM(t,n,e){let i=hn(t),r=[i[gt.MonthsFormat],i[gt.MonthsStandalone]],o=hi(r,n);return hi(o,e)}function WM(t,n){let i=hn(t)[gt.Eras];return hi(i,n)}function Lc(t,n){let e=hn(t);return hi(e[gt.DateFormat],n)}function Vc(t,n){let e=hn(t);return hi(e[gt.TimeFormat],n)}function jc(t,n){let i=hn(t)[gt.DateTimeFormat];return hi(i,n)}function xr(t,n){let e=hn(t),i=e[gt.NumberSymbols][n];if(typeof i>"u"){if(n===Pn.CurrencyDecimal)return e[gt.NumberSymbols][Pn.Decimal];if(n===Pn.CurrencyGroup)return e[gt.NumberSymbols][Pn.Group]}return i}function qM(t,n){return hn(t)[gt.NumberFormats][n]}function oV(t){return hn(t)[gt.Currencies]}function YM(t){if(!t[gt.ExtraData])throw new P(2303,!1)}function QM(t){let n=hn(t);return YM(n),(n[gt.ExtraData][2]||[]).map(i=>typeof i=="string"?ky(i):[ky(i[0]),ky(i[1])])}function ZM(t,n,e){let i=hn(t);YM(i);let r=[i[gt.ExtraData][0],i[gt.ExtraData][1]],o=hi(r,n)||[];return hi(o,e)||[]}function hi(t,n){for(let e=n;e>-1;e--)if(typeof t[e]<"u")return t[e];throw new P(2304,!1)}function ky(t){let[n,e]=t.split(":");return{hours:+n,minutes:+e}}function KM(t,n,e="en"){let i=oV(e)[t]||HM[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var aV=2;function XM(t){let n,e=HM[t];return e&&(n=e[2]),typeof n=="number"?n:aV}var sV=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Pm={},lV=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/;function JM(t,n,e,i){let r=vV(t);n=Wr(e,n)||n;let a=[],s;for(;n;)if(s=lV.exec(n),s){a=a.concat(s.slice(1));let u=a.pop();if(!u)break;n=u}else{a.push(n);break}let l=r.getTimezoneOffset();i&&(l=tI(i,l),r=gV(r,i));let c="";return a.forEach(u=>{let m=pV(u);c+=m?m(r,e,l):u==="''"?"'":u.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),c}function Bm(t,n,e){let i=new Date(0);return i.setFullYear(t,n,e),i.setHours(0,0,0),i}function Wr(t,n){let e=UM(t);if(Pm[e]??={},Pm[e][n])return Pm[e][n];let i="";switch(n){case"shortDate":i=Lc(t,Nn.Short);break;case"mediumDate":i=Lc(t,Nn.Medium);break;case"longDate":i=Lc(t,Nn.Long);break;case"fullDate":i=Lc(t,Nn.Full);break;case"shortTime":i=Vc(t,Nn.Short);break;case"mediumTime":i=Vc(t,Nn.Medium);break;case"longTime":i=Vc(t,Nn.Long);break;case"fullTime":i=Vc(t,Nn.Full);break;case"short":let r=Wr(t,"shortTime"),o=Wr(t,"shortDate");i=Fm(jc(t,Nn.Short),[r,o]);break;case"medium":let a=Wr(t,"mediumTime"),s=Wr(t,"mediumDate");i=Fm(jc(t,Nn.Medium),[a,s]);break;case"long":let l=Wr(t,"longTime"),c=Wr(t,"longDate");i=Fm(jc(t,Nn.Long),[l,c]);break;case"full":let u=Wr(t,"fullTime"),m=Wr(t,"fullDate");i=Fm(jc(t,Nn.Full),[u,m]);break}return i&&(Pm[e][n]=i),i}function Fm(t,n){return n&&(t=t.replace(/\{([^}]+)}/g,function(e,i){return n!=null&&i in n?n[i]:e})),t}function Gi(t,n,e="-",i,r){let o="";(t<0||r&&t<=0)&&(r?t=-t+1:(t=-t,o=e));let a=String(t);for(;a.length<n;)a="0"+a;return i&&(a=a.slice(a.length-n)),o+a}function cV(t,n){return Gi(t,3).substring(0,n)}function kt(t,n,e=0,i=!1,r=!1){return function(o,a){let s=dV(t,o);if((e>0||s>-e)&&(s+=e),t===3)s===0&&e===-12&&(s=12);else if(t===6)return cV(s,n);let l=xr(a,Pn.MinusSign);return Gi(s,n,l,i,r)}}function dV(t,n){switch(t){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new P(2301,!1)}}function at(t,n,e=tn.Format,i=!1){return function(r,o){return uV(r,o,t,n,e,i)}}function uV(t,n,e,i,r,o){switch(e){case 2:return GM(n,r,i)[t.getMonth()];case 1:return $M(n,r,i)[t.getDay()];case 0:let a=t.getHours(),s=t.getMinutes();if(o){let c=QM(n),u=ZM(n,r,i),m=c.findIndex(v=>{if(Array.isArray(v)){let[_,b]=v,A=a>=_.hours&&s>=_.minutes,R=a<b.hours||a===b.hours&&s<b.minutes;if(_.hours<b.hours){if(A&&R)return!0}else if(A||R)return!0}else if(v.hours===a&&v.minutes===s)return!0;return!1});if(m!==-1)return u[m]}return zM(n,r,i)[a<12?0:1];case 3:return WM(n,i)[t.getFullYear()<=0?0:1];default:let l=e;throw new P(2302,!1)}}function Lm(t){return function(n,e,i){let r=-1*i,o=xr(e,Pn.MinusSign),a=r>0?Math.floor(r/60):Math.ceil(r/60);switch(t){case 0:return(r>=0?"+":"")+Gi(a,2,o)+Gi(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+Gi(a,1,o);case 2:return"GMT"+(r>=0?"+":"")+Gi(a,2,o)+":"+Gi(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+Gi(a,2,o)+":"+Gi(Math.abs(r%60),2,o);default:throw new P(2310,!1)}}}var fV=0,jm=4;function mV(t){let n=Bm(t,fV,1).getDay();return Bm(t,0,1+(n<=jm?jm:jm+7)-n)}function eI(t){let n=t.getDay(),e=n===0?-3:jm-n;return Bm(t.getFullYear(),t.getMonth(),t.getDate()+e)}function Oy(t,n=!1){return function(e,i){let r;if(n){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,a=e.getDate();r=1+Math.floor((a+o)/7)}else{let o=eI(e),a=mV(o.getFullYear()),s=o.getTime()-a.getTime();r=1+Math.round(s/6048e5)}return Gi(r,t,xr(i,Pn.MinusSign))}}function Vm(t,n=!1){return function(e,i){let o=eI(e).getFullYear();return Gi(o,t,xr(i,Pn.MinusSign),n)}}var Ny={};function pV(t){if(Ny[t])return Ny[t];let n;switch(t){case"G":case"GG":case"GGG":n=at(3,Xe.Abbreviated);break;case"GGGG":n=at(3,Xe.Wide);break;case"GGGGG":n=at(3,Xe.Narrow);break;case"y":n=kt(0,1,0,!1,!0);break;case"yy":n=kt(0,2,0,!0,!0);break;case"yyy":n=kt(0,3,0,!1,!0);break;case"yyyy":n=kt(0,4,0,!1,!0);break;case"Y":n=Vm(1);break;case"YY":n=Vm(2,!0);break;case"YYY":n=Vm(3);break;case"YYYY":n=Vm(4);break;case"M":case"L":n=kt(1,1,1);break;case"MM":case"LL":n=kt(1,2,1);break;case"MMM":n=at(2,Xe.Abbreviated);break;case"MMMM":n=at(2,Xe.Wide);break;case"MMMMM":n=at(2,Xe.Narrow);break;case"LLL":n=at(2,Xe.Abbreviated,tn.Standalone);break;case"LLLL":n=at(2,Xe.Wide,tn.Standalone);break;case"LLLLL":n=at(2,Xe.Narrow,tn.Standalone);break;case"w":n=Oy(1);break;case"ww":n=Oy(2);break;case"W":n=Oy(1,!0);break;case"d":n=kt(2,1);break;case"dd":n=kt(2,2);break;case"c":case"cc":n=kt(7,1);break;case"ccc":n=at(1,Xe.Abbreviated,tn.Standalone);break;case"cccc":n=at(1,Xe.Wide,tn.Standalone);break;case"ccccc":n=at(1,Xe.Narrow,tn.Standalone);break;case"cccccc":n=at(1,Xe.Short,tn.Standalone);break;case"E":case"EE":case"EEE":n=at(1,Xe.Abbreviated);break;case"EEEE":n=at(1,Xe.Wide);break;case"EEEEE":n=at(1,Xe.Narrow);break;case"EEEEEE":n=at(1,Xe.Short);break;case"a":case"aa":case"aaa":n=at(0,Xe.Abbreviated);break;case"aaaa":n=at(0,Xe.Wide);break;case"aaaaa":n=at(0,Xe.Narrow);break;case"b":case"bb":case"bbb":n=at(0,Xe.Abbreviated,tn.Standalone,!0);break;case"bbbb":n=at(0,Xe.Wide,tn.Standalone,!0);break;case"bbbbb":n=at(0,Xe.Narrow,tn.Standalone,!0);break;case"B":case"BB":case"BBB":n=at(0,Xe.Abbreviated,tn.Format,!0);break;case"BBBB":n=at(0,Xe.Wide,tn.Format,!0);break;case"BBBBB":n=at(0,Xe.Narrow,tn.Format,!0);break;case"h":n=kt(3,1,-12);break;case"hh":n=kt(3,2,-12);break;case"H":n=kt(3,1);break;case"HH":n=kt(3,2);break;case"m":n=kt(4,1);break;case"mm":n=kt(4,2);break;case"s":n=kt(5,1);break;case"ss":n=kt(5,2);break;case"S":n=kt(6,1);break;case"SS":n=kt(6,2);break;case"SSS":n=kt(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Lm(0);break;case"ZZZZZ":n=Lm(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Lm(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Lm(2);break;default:return null}return Ny[t]=n,n}function tI(t,n){t=t.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+t)/6e4;return isNaN(e)?n:e}function hV(t,n){return t=new Date(t.getTime()),t.setMinutes(t.getMinutes()+n),t}function gV(t,n,e){let r=t.getTimezoneOffset(),o=tI(n,r);return hV(t,-1*(o-r))}function vV(t){if(VM(t))return t;if(typeof t=="number"&&!isNaN(t))return new Date(t);if(typeof t=="string"){if(t=t.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(t)){let[r,o=1,a=1]=t.split("-").map(s=>+s);return Bm(r,o-1,a)}let e=parseFloat(t);if(!isNaN(t-e))return new Date(e);let i;if(i=t.match(sV))return _V(i)}let n=new Date(t);if(!VM(n))throw new P(2311,!1);return n}function _V(t){let n=new Date(0),e=0,i=0,r=t[8]?n.setUTCFullYear:n.setFullYear,o=t[8]?n.setUTCHours:n.setHours;t[9]&&(e=Number(t[9]+t[10]),i=Number(t[9]+t[11])),r.call(n,Number(t[1]),Number(t[2])-1,Number(t[3]));let a=Number(t[4]||0)-e,s=Number(t[5]||0)-i,l=Number(t[6]||0),c=Math.floor(parseFloat("0."+(t[7]||0))*1e3);return o.call(n,a,s,l,c),n}function VM(t){return t instanceof Date&&!isNaN(t.valueOf())}var yV=/^(\d+)?\.((\d+)(-(\d+))?)?$/,jM=22,Hm=".",Bc="0",bV=";",CV=",",Py="#",BM="\xA4";function DV(t,n,e,i,r,o,a=!1){let s="",l=!1;if(!isFinite(t))s=xr(e,Pn.Infinity);else{let c=EV(t);a&&(c=xV(c));let u=n.minInt,m=n.minFrac,v=n.maxFrac;if(o){let _e=o.match(yV);if(_e===null)throw new P(2306,!1);let yt=_e[1],vt=_e[3],lo=_e[5];yt!=null&&(u=Fy(yt)),vt!=null&&(m=Fy(vt)),lo!=null?v=Fy(lo):vt!=null&&m>v&&(v=m);let Or=100;if(u>Or||m>Or||v>Or)throw new P(2306,!1)}SV(c,m,v);let _=c.digits,b=c.integerLen,A=c.exponent,R=[];for(l=_.every(_e=>!_e);b<u;b++)_.unshift(0);for(;b<0;b++)_.unshift(0);b>0?R=_.splice(b,_.length):(R=_,_=[0]);let L=[];for(_.length>=n.lgSize&&L.unshift(_.splice(-n.lgSize,_.length).join(""));_.length>n.gSize;)L.unshift(_.splice(-n.gSize,_.length).join(""));_.length&&L.unshift(_.join("")),s=L.join(xr(e,i)),R.length&&(s+=xr(e,r)+R.join("")),A&&(s+=xr(e,Pn.Exponential)+"+"+A)}return t<0&&!l?s=n.negPre+s+n.negSuf:s=n.posPre+s+n.posSuf,s}function nI(t,n,e,i,r){let o=qM(n,Ly.Currency),a=wV(o,xr(n,Pn.MinusSign));return a.minFrac=XM(i),a.maxFrac=a.minFrac,DV(t,a,n,Pn.CurrencyGroup,Pn.CurrencyDecimal,r).replace(BM,e).replace(BM,"").trim()}function wV(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(bV),r=i[0],o=i[1],a=r.indexOf(Hm)!==-1?r.split(Hm):[r.substring(0,r.lastIndexOf(Bc)+1),r.substring(r.lastIndexOf(Bc)+1)],s=a[0],l=a[1]||"";e.posPre=s.substring(0,s.indexOf(Py));for(let u=0;u<l.length;u++){let m=l.charAt(u);m===Bc?e.minFrac=e.maxFrac=u+1:m===Py?e.maxFrac=u+1:e.posSuf+=m}let c=s.split(CV);if(e.gSize=c[1]?c[1].length:0,e.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,m=o.indexOf(Py);e.negPre=o.substring(0,m).replace(/'/g,""),e.negSuf=o.slice(m+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function xV(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function EV(t){let n=Math.abs(t)+"",e=0,i,r,o,a,s;for((r=n.indexOf(Hm))>-1&&(n=n.replace(Hm,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===Bc;o++);if(o===(s=n.length))i=[0],r=1;else{for(s--;n.charAt(s)===Bc;)s--;for(r-=o,i=[],a=0;o<=s;o++,a++)i[a]=Number(n.charAt(o))}return r>jM&&(i=i.splice(0,jM-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function SV(t,n,e){if(n>e)throw new P(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),a=o+t.integerLen,s=i[a];if(a>0){i.splice(Math.max(t.integerLen,a));for(let m=a;m<i.length;m++)i[m]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,a=o+1),i[0]=0;for(let m=1;m<a;m++)i[m]=0}if(s>=5)if(a-1<0){for(let m=0;m>a;m--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[a-1]++;for(;r<Math.max(0,o);r++)i.push(0);let l=o!==0,c=n+t.integerLen,u=i.reduceRight(function(m,v,_,b){return v=v+m,b[_]=v<10?v:v-10,l&&(b[_]===0&&_>=c?b.pop():l=!1),v>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function Fy(t){let n=parseInt(t);if(isNaN(n))throw new P(2305,!1);return n}var zs=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(oe);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(J(ct))};static \u0275dir=M({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Pe]})}return t})();function iI(t,n){return new P(2100,!1)}var MV="mediumDate",rI=new C(""),oI=new C(""),$s=(()=>{class t{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let a=i??this.defaultOptions?.dateFormat??MV,s=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return JM(e,a,o||this.locale,s)}catch(a){throw iI(t,a.message)}}static \u0275fac=function(i){return new(i||t)(J(Hs,16),J(rI,24),J(oI,24))};static \u0275pipe=hm({name:"date",type:t,pure:!0})}return t})();var Er=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,a){if(!IV(e))return null;a||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let s=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?s=KM(s,r==="symbol"?"wide":"narrow",a):s=r);try{let l=TV(e);return nI(l,a,s,i,o)}catch(l){throw iI(t,l.message)}}static \u0275fac=function(i){return new(i||t)(J(Hs,16),J(py,16))};static \u0275pipe=hm({name:"currency",type:t,pure:!0})}return t})();function IV(t){return!(t==null||t===""||t!==t)}function TV(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new P(2309,!1);return t}var Ra=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})();function Um(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var ka=class{};var Vy="browser";function zm(t){return t===Vy}var Hc=class{_doc;constructor(n){this._doc=n}manager},$m=(()=>{class t extends Hc{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(le(V))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),qm=new C(""),Uy=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof $m));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof $m);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new P(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(le(qm),le(ie))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),jy="ng-app-id";function aI(t){for(let n of t)n.remove()}function sI(t,n){let e=n.createElement("style");return e.textContent=t,e}function RV(t,n,e,i){let r=t.head?.querySelectorAll(`style[${jy}="${n}"],link[${jy}="${n}"]`);if(r)for(let o of r)o.removeAttribute(jy),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function Hy(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var zy=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,RV(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,sI);i?.forEach(r=>this.addUsage(r,this.external,Hy))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(aI(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])aI(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,sI(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Hy(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(le(V),le(ui),le(Ta,8),le(Ur))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),By={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},$y=/%COMP%/g;var cI="%COMP%",kV=`_nghost-${cI}`,OV=`_ngcontent-${cI}`,NV=!0,PV=new C("",{factory:()=>NV});function FV(t){return OV.replace($y,t)}function LV(t){return kV.replace($y,t)}function dI(t,n){return n.map(e=>e.replace($y,t))}var Gy=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new Uc(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Wm?r.applyToHost(e):r instanceof zc&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,m=this.tracingService;switch(i.encapsulation){case Hi.Emulated:o=new Wm(l,c,i,this.appId,u,a,s,m);break;case Hi.ShadowDom:return new Gm(l,e,i,a,s,this.nonce,m,c);case Hi.ExperimentalIsolatedShadowDom:return new Gm(l,e,i,a,s,this.nonce,m);default:o=new zc(l,c,i,u,a,s,m);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(le(Uy),le(zy),le(ui),le(PV),le(V),le(ie),le(Ta),le(zi,8))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Uc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(By[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(lI(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(lI(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new P(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=By[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=By[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(yr.DashCase|yr.Important)?n.style.setProperty(e,i,r&yr.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&yr.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=pi().getGlobalEventTarget(this.doc,n),!n))throw new P(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function lI(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Gm=class extends Uc{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=dI(i.id,c);for(let m of c){let v=document.createElement("style");a&&v.setAttribute("nonce",a),v.textContent=m,this.shadowRoot.appendChild(v)}let u=i.getExternalStyles?.();if(u)for(let m of u){let v=Hy(m,r);a&&v.setAttribute("nonce",a),this.shadowRoot.appendChild(v)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},zc=class extends Uc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?dI(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Sa.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Wm=class extends zc{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=FV(c),this.hostAttr=LV(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Ym=class t extends Fc{supportsDOMEvents=!0;static makeCurrent(){Ry(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=VV();return e==null?null:jV(e)}resetBaseElement(){$c=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Um(document.cookie,n)}},$c=null;function VV(){return $c=$c||document.head.querySelector("base"),$c?$c.getAttribute("href"):null}function jV(t){return new URL(t,document.baseURI).pathname}var BV=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),uI=["alt","control","meta","shift"],HV={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},UV={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},fI=(()=>{class t extends Hc{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>pi().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),uI.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=HV[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),uI.forEach(a=>{if(a!==r){let s=UV[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(le(V))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();async function Wy(t,n,e){let i=D({rootComponent:t},zV(n,e));return TM(i)}function zV(t,n){return{platformRef:n?.platformRef,appProviders:[...YV,...t?.providers??[]],platformProviders:qV}}function $V(){Ym.makeCurrent()}function GV(){return new cn}function WV(){return s_(document),document}var qV=[{provide:Ur,useValue:Vy},{provide:qf,useValue:$V,multi:!0},{provide:V,useFactory:WV}];var YV=[{provide:ic,useValue:"root"},{provide:cn,useFactory:GV},{provide:qm,useClass:$m,multi:!0},{provide:qm,useClass:fI,multi:!0},Gy,zy,Uy,{provide:Tt,useExisting:Gy},{provide:ka,useClass:BV},[]];var Yr=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=this.headers.get(e);if(!a)return;a=a.filter(s=>o.indexOf(s)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var Yy=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Qy=class{encodeKey(n){return mI(n)}encodeValue(n){return mI(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function QV(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var ZV=/%(\d[a-f0-9])/gi,KV={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function mI(t){return encodeURIComponent(t).replace(ZV,(n,e)=>KV[e]??n)}function Qm(t){return`${t}`}var qr=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Qy,n.fromString){if(n.fromObject)throw new P(2805,!1);this.map=QV(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Qm):[Qm(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Qm(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Qm(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function XV(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function pI(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function hI(t){return typeof Blob<"u"&&t instanceof Blob}function gI(t){return typeof FormData<"u"&&t instanceof FormData}function JV(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var vI="Content-Type",_I="Accept",yI="text/plain",bI="application/json",ej=`${bI}, ${yI}, */*`,Ws=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(XV(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new P(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Yr,this.context??=new Yy,!this.params)this.params=new qr,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||pI(this.body)||hI(this.body)||gI(this.body)||JV(this.body)?this.body:this.body instanceof qr?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||gI(this.body)?null:hI(this.body)?this.body.type||null:pI(this.body)?null:typeof this.body=="string"?yI:this.body instanceof qr?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?bI:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,m=n.referrer||this.referrer,v=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,b=n.transferCache??this.transferCache,A=n.timeout??this.timeout,R=n.body!==void 0?n.body:this.body,L=n.withCredentials??this.withCredentials,_e=n.reportProgress??this.reportProgress,yt=n.headers||this.headers,vt=n.params||this.params,lo=n.context??this.context;return n.setHeaders!==void 0&&(yt=Object.keys(n.setHeaders).reduce((Or,Jo)=>Or.set(Jo,n.setHeaders[Jo]),yt)),n.setParams&&(vt=Object.keys(n.setParams).reduce((Or,Jo)=>Or.set(Jo,n.setParams[Jo]),vt)),new t(e,i,R,{params:vt,headers:yt,context:lo,reportProgress:_e,responseType:r,withCredentials:L,transferCache:b,keepalive:o,cache:s,priority:a,timeout:A,mode:l,redirect:c,credentials:u,referrer:m,integrity:v,referrerPolicy:_})}},Oa=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Oa||{}),Gc=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Yr,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Zy=class t extends Gc{constructor(n={}){super(n)}type=Oa.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Ys=class t extends Gc{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Oa.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},qs=class extends Gc{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},tj=200,nj=204;var ij=/^\)\]\}',?\n/;var rj=(()=>{class t{xhrFactory;tracingService=d(zi,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new P(-2800,!1);let i=this.xhrFactory;return X(null).pipe(mt(()=>new ye(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((R,L)=>a.setRequestHeader(R,L.join(","))),e.headers.has(_I)||a.setRequestHeader(_I,ej),!e.headers.has(vI)){let R=e.detectContentTypeHeader();R!==null&&a.setRequestHeader(vI,R)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let R=e.responseType.toLowerCase();a.responseType=R!=="json"?R:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let R=a.statusText||"OK",L=new Yr(a.getAllResponseHeaders()),_e=a.responseURL||e.url;return l=new Zy({headers:L,status:a.status,statusText:R,url:_e}),l},u=this.maybePropagateTrace(()=>{let{headers:R,status:L,statusText:_e,url:yt}=c(),vt=null;L!==nj&&(vt=typeof a.response>"u"?a.responseText:a.response),L===0&&(L=vt?tj:0);let lo=L>=200&&L<300;if(e.responseType==="json"&&typeof vt=="string"){let Or=vt;vt=vt.replace(ij,"");try{vt=vt!==""?JSON.parse(vt):null}catch(Jo){vt=Or,lo&&(lo=!1,vt={error:Jo,text:vt})}}lo?(o.next(new Ys({body:vt,headers:R,status:L,statusText:_e,url:yt||void 0})),o.complete()):o.error(new qs({error:vt,headers:R,status:L,statusText:_e,url:yt||void 0}))}),m=this.maybePropagateTrace(R=>{let{url:L}=c(),_e=new qs({error:R,status:a.status||0,statusText:a.statusText||"Unknown Error",url:L||void 0});o.error(_e)}),v=m;e.timeout&&(v=this.maybePropagateTrace(R=>{let{url:L}=c(),_e=new qs({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:L||void 0});o.error(_e)}));let _=!1,b=this.maybePropagateTrace(R=>{_||(o.next(c()),_=!0);let L={type:Oa.DownloadProgress,loaded:R.loaded};R.lengthComputable&&(L.total=R.total),e.responseType==="text"&&a.responseText&&(L.partialText=a.responseText),o.next(L)}),A=this.maybePropagateTrace(R=>{let L={type:Oa.UploadProgress,loaded:R.loaded};R.lengthComputable&&(L.total=R.total),o.next(L)});return a.addEventListener("load",u),a.addEventListener("error",m),a.addEventListener("timeout",v),a.addEventListener("abort",m),e.reportProgress&&(a.addEventListener("progress",b),s!==null&&a.upload&&a.upload.addEventListener("progress",A)),a.send(s),o.next({type:Oa.Sent}),()=>{a.removeEventListener("error",m),a.removeEventListener("abort",m),a.removeEventListener("load",u),a.removeEventListener("timeout",v),e.reportProgress&&(a.removeEventListener("progress",b),s!==null&&a.upload&&a.upload.removeEventListener("progress",A)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(le(ka))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function oj(t,n){return n(t)}function aj(t,n,e){return(i,r)=>Bt(e,()=>n(i,o=>t(o,r)))}var sj=new C("",{factory:()=>[]}),Ky=new C(""),lj=new C("",{factory:()=>!0});var cj=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=le(rj),r},providedIn:"root"})}return t})();var dj=(()=>{class t{backend;injector;chain=null;pendingTasks=d(bs);contributeToStability=d(lj);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(sj),...this.injector.get(Ky,[])]));this.chain=i.reduceRight((r,o)=>aj(r,o,this.injector),oj)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(ua(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(le(cj),le(We))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uj=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=le(dj),r},providedIn:"root"})}return t})();function qy(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var Xy=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Ws)o=e;else{let l;r.headers instanceof Yr?l=r.headers:l=new Yr(r.headers);let c;r.params&&(r.params instanceof qr?c=r.params:c=new qr({fromObject:r.params})),o=new Ws(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=X(o).pipe(da(l=>this.handler.handle(l)));if(e instanceof Ws||r.observe==="events")return a;let s=a.pipe(Ie(l=>l instanceof Ys));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(pe(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new P(2806,!1);return l.body}));case"blob":return s.pipe(pe(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new P(2807,!1);return l.body}));case"text":return s.pipe(pe(l=>{if(l.body!==null&&typeof l.body!="string")throw new P(2808,!1);return l.body}));default:return s.pipe(pe(l=>l.body))}case"response":return s;default:throw new P(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new qr().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,qy(r,i))}post(e,i,r={}){return this.request("POST",e,qy(r,i))}put(e,i,r={}){return this.request("PUT",e,qy(r,i))}static \u0275fac=function(i){return new(i||t)(le(uj))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var fj=new C(""),mj="b",pj="h",hj="s",gj="st",vj="u",_j="rt",Jy=new C(""),yj=["GET","HEAD"];function wI(t,n){let a=n,{isCacheActive:e}=a,i=vC(a,["isCacheActive"]),{transferCache:r,method:o}=t;return!(!e||r===!1||t.withCredentials||o==="POST"&&!i.includePostRequests&&!r||o!=="POST"&&!yj.includes(o)||!i.includeRequestsWithAuthHeaders&&Dj(t)||i.filter?.(t)===!1)}function xI(t,n){let{includeHeaders:e}=t,i=e;return typeof n=="object"&&n.includeHeaders&&(i=n.includeHeaders),i}function bj(t,n,e,i){let{transferCache:r}=t;if(!wI(t,n))return null;if(i)throw new P(2803,!1);let o=t.url,a=EI(t,o),s=e.get(a,null),l=xI(n,r);if(s){let{[mj]:c,[_j]:u,[pj]:m,[hj]:v,[gj]:_,[vj]:b}=s,A=c;switch(u){case"arraybuffer":A=DI(c);break;case"blob":A=new Blob([DI(c)]);break}let R=new Yr(m);return new Ys({body:A,headers:R,status:v,statusText:_,url:b})}return null}function Cj(t,n){let e=d(Jy),i=d(As),r=d(fj,{optional:!0}),o=bj(t,e,i,r);if(o)return X(o);let{transferCache:a}=t,s=xI(e,a),l=t.url,c=EI(t,l);return wI(t,e),n(t)}function Dj(t){return t.headers.has("authorization")||t.headers.has("proxy-authorization")||t.headers.has("cookie")}function CI(t){return[...t.keys()].sort().map(n=>`${n}=${t.getAll(n)}`).join("&")}function EI(t,n){let{params:e,method:i,responseType:r}=t,o=CI(e),a=t.serializeBody();a instanceof URLSearchParams?a=CI(a):typeof a!="string"&&(a="");let s=[i,r,n,a,o].join("|"),l=wj(s);return l}function wj(t){let n=0;for(let e of t)n=Math.imul(31,n)+e.charCodeAt(0)<<0;return n+=2147483648,n.toString()}function DI(t){let n=atob(t);return Uint8Array.from(n,i=>i.charCodeAt(0)).buffer}function SI(t){return[{provide:Jy,useFactory:()=>(fi("NgHttpTransferCache"),D({isCacheActive:!0},t))},{provide:Ky,useValue:Cj,multi:!0},{provide:Ao,multi:!0,useFactory:()=>{let n=d(At),e=d(Jy);return()=>{n.whenStable().then(()=>{e.isCacheActive=!1})}}}]}var MI=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(le(V))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Wc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=le(Ej),r},providedIn:"root"})}return t})(),Ej=(()=>{class t extends Wc{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Ft.NONE:return i;case Ft.HTML:return zr(i,"HTML")?Ui(i):D_(this._doc,String(i)).toString();case Ft.STYLE:return zr(i,"Style")?Ui(i):i;case Ft.SCRIPT:if(zr(i,"Script"))return Ui(i);throw new P(5200,!1);case Ft.URL:return zr(i,"URL")?Ui(i):Mc(String(i));case Ft.RESOURCE_URL:if(zr(i,"ResourceURL"))return Ui(i);throw new P(5201,!1);default:throw new P(5202,!1)}}bypassSecurityTrustHtml(e){return v_(e)}bypassSecurityTrustStyle(e){return __(e)}bypassSecurityTrustScript(e){return y_(e)}bypassSecurityTrustUrl(e){return b_(e)}bypassSecurityTrustResourceUrl(e){return C_(e)}static \u0275fac=function(i){return new(i||t)(le(V))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Zm=(function(t){return t[t.NoHttpTransferCache=0]="NoHttpTransferCache",t[t.HttpTransferCacheOptions=1]="HttpTransferCacheOptions",t[t.I18nSupport=2]="I18nSupport",t[t.EventReplay=3]="EventReplay",t[t.IncrementalHydration=4]="IncrementalHydration",t})(Zm||{});function Sj(t,n=[],e={}){return{\u0275kind:t,\u0275providers:n}}function II(){return Sj(Zm.EventReplay,MM())}function TI(...t){let n=[],e=new Set;for(let{\u0275providers:r,\u0275kind:o}of t)e.add(o),r.length&&n.push(r);let i=e.has(Zm.HttpTransferCacheOptions);return fr([[],[],IM(),e.has(Zm.NoHttpTransferCache)||i?[]:SI({}),n])}var xe="primary",sd=Symbol("RouteTitle"),rb=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Pa(t){return new rb(t)}function eb(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function LI(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return eb(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!eb(o,t.slice(0,o.length),s)||!eb(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function np(t){return new Promise((n,e)=>{t.pipe(Fr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function Mj(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Sr(t[e],n[e]))return!1;return!0}function Sr(t,n){let e=t?ob(t):void 0,i=n?ob(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!VI(t[r],n[r]))return!1;return!0}function ob(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function VI(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function Ij(t){return t.length>0?t[t.length-1]:null}function Va(t){return la(t)?t:To(t)?st(Promise.resolve(t)):X(t)}function jI(t){return la(t)?np(t):Promise.resolve(t)}var Tj={exact:HI,subset:UI},BI={exact:Aj,subset:Rj,ignored:()=>!0},yb={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Xc={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function bb(t,n,e){let i=t instanceof gn?t:n.parseUrl(t);return Rt(()=>ab(n.lastSuccessfulNavigation()?.finalUrl??new gn,i,D(D({},Xc),e)))}function ab(t,n,e){return Tj[e.paths](t.root,n.root,e.matrixParams)&&BI[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function Aj(t,n){return Sr(t,n)}function HI(t,n,e){if(!Na(t.segments,n.segments)||!Jm(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!HI(t.children[i],n.children[i],e))return!1;return!0}function Rj(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>VI(t[e],n[e]))}function UI(t,n,e){return zI(t,n,n.segments,e)}function zI(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Na(r,e)||n.hasChildren()||!Jm(r,e,i))}else if(t.segments.length===e.length){if(!Na(t.segments,e)||!Jm(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!UI(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Na(t.segments,r)||!Jm(t.segments,r,i)||!t.children[xe]?!1:zI(t.children[xe],n,o,i)}}function Jm(t,n,e){return n.every((i,r)=>BI[e](t[r].parameters,i.parameters))}var gn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new $e([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Pa(this.queryParams),this._queryParamMap}toString(){return Nj.serialize(this)}},$e=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return ep(this)}},Po=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Pa(this.parameters),this._parameterMap}toString(){return GI(this)}};function kj(t,n){return Na(t,n)&&t.every((e,i)=>Sr(e.parameters,n[i].parameters))}function Na(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function Oj(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===xe&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==xe&&(e=e.concat(n(r,i)))}),e}var il=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new Fo,providedIn:"root"})}return t})(),Fo=class{parse(n){let e=new lb(n);return new gn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${qc(n.root,!0)}`,i=Lj(n.queryParams),r=typeof n.fragment=="string"?`#${Pj(n.fragment)}`:"";return`${e}${i}${r}`}},Nj=new Fo;function ep(t){return t.segments.map(n=>GI(n)).join("/")}function qc(t,n){if(!t.hasChildren())return ep(t);if(n){let e=t.children[xe]?qc(t.children[xe],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==xe&&i.push(`${r}:${qc(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=Oj(t,(i,r)=>r===xe?[qc(t.children[xe],!1)]:[`${r}:${qc(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[xe]!=null?`${ep(t)}/${e[0]}`:`${ep(t)}/(${e.join("//")})`}}function $I(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Km(t){return $I(t).replace(/%3B/gi,";")}function Pj(t){return encodeURI(t)}function sb(t){return $I(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function tp(t){return decodeURIComponent(t)}function RI(t){return tp(t.replace(/\+/g,"%20"))}function GI(t){return`${sb(t.path)}${Fj(t.parameters)}`}function Fj(t){return Object.entries(t).map(([n,e])=>`;${sb(n)}=${sb(e)}`).join("")}function Lj(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Km(e)}=${Km(r)}`).join("&"):`${Km(e)}=${Km(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var Vj=/^[^\/()?;#]+/;function tb(t){let n=t.match(Vj);return n?n[0]:""}var jj=/^[^\/()?;=#]+/;function Bj(t){let n=t.match(jj);return n?n[0]:""}var Hj=/^[^=?&#]+/;function Uj(t){let n=t.match(Hj);return n?n[0]:""}var zj=/^[^&#]+/;function $j(t){let n=t.match(zj);return n?n[0]:""}var lb=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new $e([],{}):new $e([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new P(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[xe]=new $e(e,i)),r}parseSegment(){let n=tb(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new P(4009,!1);return this.capture(n),new Po(tp(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=Bj(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=tb(this.remaining);r&&(i=r,this.capture(i))}n[tp(e)]=tp(i)}parseQueryParam(n){let e=Uj(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=$j(this.remaining);a&&(i=a,this.capture(i))}let r=RI(e),o=RI(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=tb(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new P(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=xe);let s=this.parseChildren(e+1);i[a??xe]=Object.keys(s).length===1&&s[xe]?s[xe]:new $e([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new P(4011,!1)}};function WI(t){return t.segments.length>0?new $e([],{[xe]:t}):t}function qI(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=qI(r);if(i===xe&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new $e(t.segments,n);return Gj(e)}function Gj(t){if(t.numberOfChildren===1&&t.children[xe]){let n=t.children[xe];return new $e(t.segments.concat(n.segments),n.children)}return t}function Lo(t){return t instanceof gn}function YI(t,n,e=null,i=null,r=new Fo){let o=QI(t);return ZI(o,n,e,i,r)}function QI(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new $e(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=WI(i);return n??r}function ZI(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return nb(o,o,o,e,i,r);let a=Wj(n);if(a.toRoot())return nb(o,o,new $e([],{}),e,i,r);let s=qj(a,o,t),l=s.processChildren?Qc(s.segmentGroup,s.index,a.commands):XI(s.segmentGroup,s.index,a.commands);return nb(o,s.segmentGroup,l,e,i,r)}function ip(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Jc(t){return typeof t=="object"&&t!=null&&t.outlets}function kI(t,n,e){t||="\u0275";let i=new gn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function nb(t,n,e,i,r,o){let a={};for(let[c,u]of Object.entries(i??{}))a[c]=Array.isArray(u)?u.map(m=>kI(c,m,o)):kI(c,u,o);let s;t===n?s=e:s=KI(t,n,e);let l=WI(qI(s));return new gn(l,a,r)}function KI(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=KI(o,n,e)}),new $e(t.segments,i)}var rp=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&ip(i[0]))throw new P(4003,!1);let r=i.find(Jc);if(r&&r!==Ij(i))throw new P(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function Wj(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new rp(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new rp(e,n,i)}var Zs=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function qj(t,n,e){if(t.isAbsolute)return new Zs(n,!0,0);if(!e)return new Zs(n,!1,NaN);if(e.parent===null)return new Zs(e,!0,0);let i=ip(t.commands[0])?0:1,r=e.segments.length-1+i;return Yj(e,r,t.numberOfDoubleDots)}function Yj(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new P(4005,!1);r=i.segments.length}return new Zs(i,!1,r-o)}function Qj(t){return Jc(t[0])?t[0].outlets:{[xe]:t}}function XI(t,n,e){if(t??=new $e([],{}),t.segments.length===0&&t.hasChildren())return Qc(t,n,e);let i=Zj(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new $e(t.segments.slice(0,i.pathIndex),{});return o.children[xe]=new $e(t.segments.slice(i.pathIndex),t.children),Qc(o,0,r)}else return i.match&&r.length===0?new $e(t.segments,{}):i.match&&!t.hasChildren()?cb(t,n,e):i.match?Qc(t,0,r):cb(t,n,e)}function Qc(t,n,e){if(e.length===0)return new $e(t.segments,{});{let i=Qj(e),r={};if(Object.keys(i).some(o=>o!==xe)&&t.children[xe]&&t.numberOfChildren===1&&t.children[xe].segments.length===0){let o=Qc(t.children[xe],n,e);return new $e(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=XI(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new $e(t.segments,r)}}function Zj(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(Jc(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!NI(l,c,a))return o;i+=2}else{if(!NI(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function cb(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Jc(o)){let l=Kj(o.outlets);return new $e(i,l)}if(r===0&&ip(e[0])){let l=t.segments[n];i.push(new Po(l.path,OI(e[0]))),r++;continue}let a=Jc(o)?o.outlets[xe]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&ip(s)?(i.push(new Po(a,OI(s))),r+=2):(i.push(new Po(a,{})),r++)}return new $e(i,{})}function Kj(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=cb(new $e([],{}),0,i))}),n}function OI(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function NI(t,n,e){return t==e.path&&Sr(n,e.parameters)}var Zc="imperative",Ut=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Ut||{}),ei=class{id;url;constructor(n,e){this.id=n,this.url=e}},Fa=class extends ei{type=Ut.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},qi=class extends ei{urlAfterRedirects;type=Ut.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},nn=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(nn||{}),ed=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(ed||{}),gi=class extends ei{reason;code;type=Ut.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function JI(t){return t instanceof gi&&(t.code===nn.Redirect||t.code===nn.SupersededByNewNavigation)}var Zr=class extends ei{reason;code;type=Ut.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},La=class extends ei{error;target;type=Ut.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},td=class extends ei{urlAfterRedirects;state;type=Ut.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},op=class extends ei{urlAfterRedirects;state;type=Ut.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ap=class extends ei{urlAfterRedirects;state;shouldActivate;type=Ut.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},sp=class extends ei{urlAfterRedirects;state;type=Ut.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},lp=class extends ei{urlAfterRedirects;state;type=Ut.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},cp=class{route;type=Ut.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},dp=class{route;type=Ut.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},up=class{snapshot;type=Ut.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},fp=class{snapshot;type=Ut.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},mp=class{snapshot;type=Ut.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},pp=class{snapshot;type=Ut.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Xs=class{},nd=class{},Js=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function Xj(t){return!(t instanceof Xs)&&!(t instanceof Js)&&!(t instanceof nd)}var hp=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new rl(this.rootInjector)}},rl=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new hp(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(le(We))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gp=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=db(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=db(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=ub(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return ub(n,this._root).map(e=>e.value)}};function db(t,n){if(t===n.value)return n;for(let e of n.children){let i=db(t,e);if(i)return i}return null}function ub(t,n){if(t===n.value)return[n];for(let e of n.children){let i=ub(t,e);if(i.length)return i.unshift(n),i}return[]}var Jn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function Qs(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var id=class extends gp{snapshot;constructor(n,e){super(n),this.snapshot=e,Db(this,n)}toString(){return this.snapshot.toString()}};function eT(t,n){let e=Jj(t,n),i=new Ct([new Po("",{})]),r=new Ct({}),o=new Ct({}),a=new Ct({}),s=new Ct(""),l=new Kr(i,r,a,s,o,xe,t,e.root);return l.snapshot=e.root,new id(new Jn(l,[]),e)}function Jj(t,n){let e={},i={},r={},a=new el([],e,r,"",i,xe,t,null,{},n);return new rd("",new Jn(a,[]))}var Kr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(pe(c=>c[sd]))??X(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(pe(n=>Pa(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(pe(n=>Pa(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Cb(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:D(D({},n.params),t.params),data:D(D({},n.data),t.data),resolve:D(D(D(D({},t.data),n.data),r?.data),t._resolvedData)}:i={params:D({},t.params),data:D({},t.data),resolve:D(D({},t.data),t._resolvedData??{})},r&&nT(r)&&(i.resolve[sd]=r.title),i}var el=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[sd]}constructor(n,e,i,r,o,a,s,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Pa(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Pa(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},rd=class extends gp{url;constructor(n,e){super(e),this.url=n,Db(this,e)}toString(){return tT(this._root)}};function Db(t,n){n.value._routerState=t,n.children.forEach(e=>Db(t,e))}function tT(t){let n=t.children.length>0?` { ${t.children.map(tT).join(", ")} } `:"";return`${t.value}${n}`}function ib(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Sr(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Sr(n.params,e.params)||t.paramsSubject.next(e.params),Mj(n.url,e.url)||t.urlSubject.next(e.url),Sr(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function fb(t,n){let e=Sr(t.params,n.params)&&kj(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||fb(t.parent,n.parent))}function nT(t){return typeof t.title=="string"||t.title===null}var iT=new C(""),ja=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=xe;activateEvents=new Q;deactivateEvents=new Q;attachEvents=new Q;detachEvents=new Q;routerOutletData=km();parentContexts=d(rl);location=d(ct);changeDetector=d(Le);inputBinder=d(ld,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new P(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new P(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new P(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new P(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new mb(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Pe]})}return t})(),mb=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Kr?this.route:n===rl?this.childContexts:n===iT?this.outletData:this.parent.get(n,e)}},ld=new C(""),wb=(()=>{class t{outletDataSubscriptions=new Map;bindActivatedRouteToOutletComponent(e){this.unsubscribeFromRouteData(e),this.subscribeToRouteData(e)}unsubscribeFromRouteData(e){this.outletDataSubscriptions.get(e)?.unsubscribe(),this.outletDataSubscriptions.delete(e)}subscribeToRouteData(e){let{activatedRoute:i}=e,r=sr([i.queryParams,i.params,i.data]).pipe(mt(([o,a,s],l)=>(s=D(D(D({},o),a),s),l===0?X(s):Promise.resolve(s)))).subscribe(o=>{if(!e.isActivated||!e.activatedComponentRef||e.activatedRoute!==i||i.component===null){this.unsubscribeFromRouteData(e);return}let a=kM(i.component);if(!a){this.unsubscribeFromRouteData(e);return}for(let{templateName:s}of a.inputs)e.activatedComponentRef.setInput(s,o[s])});this.outletDataSubscriptions.set(e,r)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),xb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&U(0,"router-outlet")},dependencies:[ja],encapsulation:2})}return t})();function Eb(t){let n=t.children&&t.children.map(Eb),e=n?fe(D({},t),{children:n}):D({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==xe&&(e.component=xb),e}function eB(t,n,e){let i=od(t,n._root,e?e._root:void 0);return new id(i,n)}function od(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=tB(t,n,e);return new Jn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>od(t,s)),a}}let i=nB(n.value),r=n.children.map(o=>od(t,o));return new Jn(i,r)}}function tB(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return od(t,i,r);return od(t,i)})}function nB(t){return new Kr(new Ct(t.url),new Ct(t.params),new Ct(t.queryParams),new Ct(t.fragment),new Ct(t.data),t.outlet,t.component,t)}var tl=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},rT="ngNavigationCancelingError";function vp(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=Lo(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=oT(!1,nn.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function oT(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[rT]=!0,e.cancellationCode=n,e}function iB(t){return aT(t)&&Lo(t.url)}function aT(t){return!!t&&t[rT]}var pb=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),ib(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=Qs(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Qs(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Qs(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=Qs(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new pp(o.value.snapshot))}),n.children.length&&this.forwardEvent(new fp(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(ib(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),ib(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},_p=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Ks=class{component;route;constructor(n,e){this.component=n,this.route=e}};function rB(t,n,e){let i=t._root,r=n?n._root:null;return Yc(i,r,e,[i.value])}function oB(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function ol(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!vg(t)?t:n.get(t):i}function Yc(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Qs(n);return t.children.forEach(a=>{aB(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>Kc(s,e.getContext(a),r)),r}function aB(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=sB(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new _p(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?Yc(t,n,s?s.children:null,i,r):Yc(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new Ks(s.outlet.component,a))}else a&&Kc(n,s,r),r.canActivateChecks.push(new _p(i)),o.component?Yc(t,null,s?s.children:null,i,r):Yc(t,null,e,i,r);return r}function sB(t,n,e){if(typeof e=="function")return Bt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Na(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Na(t.url,n.url)||!Sr(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!fb(t,n)||!Sr(t.queryParams,n.queryParams);default:return!fb(t,n)}}function Kc(t,n,e){let i=Qs(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?Kc(a,n.children.getContext(o),e):Kc(a,null,e):Kc(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Ks(n.outlet.component,r)):e.canDeactivateChecks.push(new Ks(null,r)):e.canDeactivateChecks.push(new Ks(null,r))}function cd(t){return typeof t=="function"}function lB(t){return typeof t=="boolean"}function cB(t){return t&&cd(t.canLoad)}function dB(t){return t&&cd(t.canActivate)}function uB(t){return t&&cd(t.canActivateChild)}function fB(t){return t&&cd(t.canDeactivate)}function mB(t){return t&&cd(t.canMatch)}function sT(t){return t instanceof ca||t?.name==="EmptyError"}var Xm=Symbol("INITIAL_VALUE");function nl(){return mt(t=>sr(t.map(n=>n.pipe(lt(1),qe(Xm)))).pipe(pe(n=>{for(let e of n)if(e!==!0){if(e===Xm)return Xm;if(e===!1||pB(e))return e}return!0}),Ie(n=>n!==Xm),lt(1)))}function pB(t){return Lo(t)||t instanceof tl}function lT(t){return t.aborted?X(void 0).pipe(lt(1)):new ye(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function cT(t){return Te(lT(t))}function hB(t){return Kt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?X(fe(D({},n),{guardsResult:!0})):gB(o,e,i).pipe(Kt(a=>a&&lB(a)?vB(e,r,t):X(a)),pe(a=>fe(D({},n),{guardsResult:a})))})}function gB(t,n,e){return st(t).pipe(Kt(i=>DB(i.component,i.route,e,n)),Fr(i=>i!==!0,!0))}function vB(t,n,e){return st(n).pipe(da(i=>_o(yB(i.route.parent,e),_B(i.route,e),CB(t,i.path),bB(t,i.route))),Fr(i=>i!==!0,!0))}function _B(t,n){return t!==null&&n&&n(new mp(t)),X(!0)}function yB(t,n){return t!==null&&n&&n(new up(t)),X(!0)}function bB(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return X(!0);let i=e.map(r=>Ai(()=>{let o=n._environmentInjector,a=ol(r,o),s=dB(a)?a.canActivate(n,t):Bt(o,()=>a(n,t));return Va(s).pipe(Fr())}));return X(i).pipe(nl())}function CB(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>oB(o)).filter(o=>o!==null).map(o=>Ai(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=ol(s,l),u=uB(c)?c.canActivateChild(e,t):Bt(l,()=>c(e,t));return Va(u).pipe(Fr())});return X(a).pipe(nl())}));return X(r).pipe(nl())}function DB(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return X(!0);let o=r.map(a=>{let s=n._environmentInjector,l=ol(a,s),c=fB(l)?l.canDeactivate(t,n,e,i):Bt(s,()=>l(t,n,e,i));return Va(c).pipe(Fr())});return X(o).pipe(nl())}function wB(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return X(!0);let a=o.map(s=>{let l=ol(s,t),c=cB(l)?l.canLoad(n,e):Bt(t,()=>l(n,e)),u=Va(c);return r?u.pipe(cT(r)):u});return X(a).pipe(nl(),dT(i))}function dT(t){return qh(jt(n=>{if(typeof n!="boolean")throw vp(t,n)}),pe(n=>n===!0))}function xB(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return X(!0);let s=a.map(l=>{let c=ol(l,t),u=mB(c)?c.canMatch(n,e,r):Bt(t,()=>c(n,e,r));return Va(u).pipe(cT(o))});return X(s).pipe(nl(),dT(i))}var Qr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},ad=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function EB(t){throw new P(4e3,!1)}function SB(t){throw oT(!1,nn.GuardRejected)}var hb=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[xe])throw EB(`${n.redirectTo}`);r=r.children[xe]}}async applyRedirectCommands(n,e,i,r,o){let a=await MB(e,r,o);if(a instanceof gn)throw new ad(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new ad(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new gn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new $e(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new P(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function MB(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return np(Va(Bt(e,()=>i(n))))}function IB(t,n){return t.providers&&!t._injector&&(t._injector=Ps(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Wi(t){return t.outlet||xe}function TB(t,n){let e=t.filter(i=>Wi(i)===n);return e.push(...t.filter(i=>Wi(i)!==n)),e}var gb={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function uT(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function AB(t,n,e,i,r,o,a){let s=fT(t,n,e);if(!s.matched)return X(s);let l=uT(o(s));return i=IB(n,i),xB(i,n,e,r,l,a).pipe(pe(c=>c===!0?s:D({},gb)))}function fT(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?D({},gb):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||LI)(e,t,n);if(!r)return D({},gb);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?D(D({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function PI(t,n,e,i,r){return e.length>0&&OB(t,e,i,r)?{segmentGroup:new $e(n,kB(i,new $e(e,t.children))),slicedSegments:[]}:e.length===0&&NB(t,e,i)?{segmentGroup:new $e(t.segments,RB(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new $e(t.segments,t.children),slicedSegments:e}}function RB(t,n,e,i){let r={};for(let o of e)if(bp(t,n,o)&&!i[Wi(o)]){let a=new $e([],{});r[Wi(o)]=a}return D(D({},i),r)}function kB(t,n){let e={};e[xe]=n;for(let i of t)if(i.path===""&&Wi(i)!==xe){let r=new $e([],{});e[Wi(i)]=r}return e}function OB(t,n,e,i){return e.some(r=>!bp(t,n,r)||!(Wi(r)!==xe)?!1:!(i!==void 0&&Wi(r)===i))}function NB(t,n,e){return e.some(i=>bp(t,n,i))}function bp(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function PB(t,n,e){return n.length===0&&!t.children[e]}var vb=class{};async function FB(t,n,e,i,r,o,a="emptyOnly",s){return new _b(t,n,e,i,r,a,o,s).recognize()}var LB=31,_b=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new hb(this.urlSerializer,this.urlTree)}noMatchError(n){return new P(4002,`'${n.segmentGroup}'`)}async recognize(){let n=PI(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Jn(i,e),o=new rd("",r),a=YI(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new el([],Object.freeze({}),Object.freeze(D({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),xe,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,xe,e),rootSnapshot:e}}catch(i){if(i instanceof ad)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Qr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof Jn?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],u=TB(e,l),m=await this.processSegmentGroup(n,u,c,l,r);a.push(...m)}let s=mT(a);return VB(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof Qr||sT(c))continue;throw c}if(PB(i,r,o))return new vb;throw new Qr(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(Wi(i)!==a&&(a===xe||!bp(r,o,i)))throw new Qr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new Qr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:m,remainingSegments:v}=fT(e,r,o);if(!l)throw new Qr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>LB&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let b=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,m,uT(_),n),A=await this.applyRedirects.lineralizeSegments(r,b);return this.processSegment(n,i,e,A.concat(v),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new el(i,r,Object.freeze(D({},this.urlTree.queryParams)),this.urlTree.fragment,BB(e),Wi(e),e.component??e._loadedComponent??null,e,HB(e),n),s=Cb(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=yt=>this.createSnapshot(n,i,yt.consumedSegments,yt.parameters,a),l=await np(AB(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new Qr(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:m,consumedSegments:v,remainingSegments:_}=l,b=this.createSnapshot(n,i,v,m,a),{segmentGroup:A,slicedSegments:R}=PI(e,v,_,c,o);if(R.length===0&&A.hasChildren()){let yt=await this.processChildren(u,c,A,b);return new Jn(b,yt)}if(c.length===0&&R.length===0)return new Jn(b,[]);let L=Wi(i)===o,_e=await this.processSegment(u,c,A,R,L?xe:o,!0,b);return new Jn(b,_e instanceof Jn?[_e]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await np(wB(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw SB(e)}return{routes:[],injector:n}}};function VB(t){t.sort((n,e)=>n.value.outlet===xe?-1:e.value.outlet===xe?1:n.value.outlet.localeCompare(e.value.outlet))}function jB(t){let n=t.value.routeConfig;return n&&n.path===""}function mT(t){let n=[],e=new Set;for(let i of t){if(!jB(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=mT(i.children);n.push(new Jn(i.value,r))}return n.filter(i=>!e.has(i))}function BB(t){return t.data||{}}function HB(t){return t.resolve||{}}function UB(t,n,e,i,r,o,a){return Kt(async s=>{let{state:l,tree:c}=await FB(t,n,e,i,s.extractedUrl,r,o,a);return fe(D({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function zB(t){return Kt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return X(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of pT(s))o.add(l);let a=0;return st(o).pipe(da(s=>r.has(s)?$B(s,e,t):(s.data=Cb(s,s.parent,t).resolve,X(void 0))),jt(()=>a++),Lu(1),Kt(s=>a===o.size?X(n):ft))})}function pT(t){let n=t.children.map(e=>pT(e)).flat();return[t,...n]}function $B(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!nT(i)&&(r[sd]=i.title),Ai(()=>(t.data=Cb(t,t.parent,e).resolve,GB(r,t,n).pipe(pe(o=>(t._resolvedData=o,t.data=D(D({},t.data),o),null)))))}function GB(t,n,e){let i=ob(t);if(i.length===0)return X({});let r={};return st(i).pipe(Kt(o=>WB(t[o],n,e).pipe(Fr(),jt(a=>{if(a instanceof tl)throw vp(new Fo,a);r[o]=a}))),Lu(1),pe(()=>r),yo(o=>sT(o)?ft:Ul(o)))}function WB(t,n,e){let i=n._environmentInjector,r=ol(t,i),o=r.resolve?r.resolve(n,e):Bt(i,()=>r(n,e));return Va(o)}function FI(t){return mt(n=>{let e=t(n);return e?st(e).pipe(pe(()=>n)):X(n)})}var Sb=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===xe);return i}getResolvedTitleForRoute(e){return e.data[sd]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(hT),providedIn:"root"})}return t})(),hT=(()=>{class t extends Sb{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(le(MI))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),al=new C("",{factory:()=>({})}),dd=new C(""),gT=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(my);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await jI(Bt(e,()=>i.loadComponent())),a=await yT(_T(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await vT(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function vT(t,n,e,i){let r=await jI(Bt(e,()=>t.loadChildren())),o=await yT(_T(r)),a;o instanceof pm||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,u;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,u=a,l=s.get(dd,[],{optional:!0,self:!0}).flat()),{routes:l.map(Eb),injector:s,factory:u}}function qB(t){return t&&typeof t=="object"&&"default"in t}function _T(t){return qB(t)?t.default:t}async function yT(t){return t}var Cp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(YB),providedIn:"root"})}return t})(),YB=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),bT=new C("");var QB=()=>{},CT=new C(""),DT=(()=>{class t{currentNavigation=S(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=S(null);events=new k;transitionAbortWithErrorSubject=new k;configLoader=d(gT);environmentInjector=d(We);destroyRef=d(pn);urlSerializer=d(il);rootContexts=d(rl);location=d(No);inputBindingEnabled=d(ld,{optional:!0})!==null;titleStrategy=d(Sb);options=d(al,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(Cp);createViewTransition=d(bT,{optional:!0});navigationErrorHandler=d(CT,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>X(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new cp(r)),i=r=>this.events.next(new dp(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;He(()=>{this.transitions?.next(fe(D({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Ct(null),this.transitions.pipe(Ie(i=>i!==null),mt(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return X(i).pipe(mt(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",nn.SupersededByNewNavigation),ft;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?fe(D({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new Zr(s.id,this.urlSerializer.serialize(s.rawUrl),"",ed.IgnoredSameUrlNavigation)),s.resolve(!1),ft;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return X(s).pipe(mt(m=>(this.events.next(new Fa(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?ft:Promise.resolve(m))),UB(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),jt(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(v=>(v.finalUrl=m.urlAfterRedirects,v)),this.events.next(new nd)}),mt(m=>st(i.routesRecognizeHandler.deferredHandle??X(void 0)).pipe(pe(()=>m))),jt(()=>{let m=new td(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(m)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:m,extractedUrl:v,source:_,restoredState:b,extras:A}=s,R=new Fa(m,this.urlSerializer.serialize(v),_,b);this.events.next(R);let L=eT(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=fe(D({},s),{targetSnapshot:L,urlAfterRedirects:v,extras:fe(D({},A),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(_e=>(_e.finalUrl=v,_e)),X(i)}else return this.events.next(new Zr(s.id,this.urlSerializer.serialize(s.extractedUrl),"",ed.IgnoredByUrlHandlingStrategy)),s.resolve(!1),ft}),pe(s=>{let l=new op(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=fe(D({},s),{guards:rB(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),hB(s=>this.events.next(s)),mt(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw vp(this.urlSerializer,s.guardsResult);let l=new ap(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return ft;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",nn.GuardRejected),ft;if(s.guards.canActivateChecks.length===0)return X(s);let c=new sp(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return ft;let u=!1;return X(s).pipe(zB(this.paramsInheritanceStrategy),jt({next:()=>{u=!0;let m=new lp(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(m)},complete:()=>{u||this.cancelNavigationTransition(s,"",nn.NoDataFromResolver)}}))}),FI(s=>{let l=u=>{let m=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let v=u._environmentInjector;m.push(this.configLoader.loadComponent(v,u.routeConfig).then(_=>{u.component=_}))}for(let v of u.children)m.push(...l(v));return m},c=l(s.targetSnapshot.root);return c.length===0?X(s):st(Promise.all(c).then(()=>s))}),FI(()=>this.afterPreactivation()),mt(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?st(c).pipe(pe(()=>i)):X(i)}),lt(1),mt(s=>{let l=eB(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=fe(D({},s),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new Xs);let c=i.beforeActivateHandler.deferredHandle;return c?st(c.then(()=>s)):X(s)}),jt(s=>{new pb(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=QB,l)),this.lastSuccessfulNavigation.set(He(this.currentNavigation)),this.events.next(new qi(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),Te(lT(o.signal).pipe(Ie(()=>!r&&!i.targetRouterState),jt(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",nn.Aborted)}))),jt({complete:()=>{r=!0}}),Te(this.transitionAbortWithErrorSubject.pipe(jt(s=>{throw s}))),ua(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",nn.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),yo(s=>{if(r=!0,this.destroyed)return i.resolve(!1),ft;if(aT(s))this.events.next(new gi(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),iB(s)?this.events.next(new Js(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new La(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=Bt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof tl){let{message:u,cancellationCode:m}=vp(this.urlSerializer,c);this.events.next(new gi(i.id,this.urlSerializer.serialize(i.extractedUrl),u,m)),this.events.next(new Js(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return ft}))}))}cancelNavigationTransition(e,i,r){let o=new gi(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=He(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ZB(t){return t!==Zc}var wT=new C("");var xT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(KB),providedIn:"root"})}return t})(),yp=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},KB=(()=>{class t extends yp{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Dp=(()=>{class t{urlSerializer=d(il);options=d(al,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(No);urlHandlingStrategy=d(Cp);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new gn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof gn?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=eT(null,d(We));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(XB),providedIn:"root"})}return t})(),XB=(()=>{class t extends Dp{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Fa?this.updateStateMemento():e instanceof Zr?this.commitTransition(i):e instanceof td?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Xs?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof gi&&!JI(e)?this.restoreHistory(i):e instanceof La?this.restoreHistory(i,!0):e instanceof qi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=D(D({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=D(D({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?D({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):D({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Mb(t,n){t.events.pipe(Ie(e=>e instanceof qi||e instanceof gi||e instanceof La||e instanceof Zr),pe(e=>e instanceof qi||e instanceof Zr?0:(e instanceof gi?e.code===nn.Redirect||e.code===nn.SupersededByNewNavigation:!1)?2:1),Ie(e=>e!==2),lt(1)).subscribe(()=>{n()})}var vn=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(gm);stateManager=d(Dp);options=d(al,{optional:!0})||{};pendingTasks=d(vr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(DT);urlSerializer=d(il);location=d(No);urlHandlingStrategy=d(Cp);injector=d(We);_events=new k;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(xT);injectorCleanup=d(wT,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(dd,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(ld,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new me;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=He(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof gi&&i.code!==nn.Redirect&&i.code!==nn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof qi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Js){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=D({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||ZB(r.source)},a);this.scheduleNavigation(s,Zc,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}Xj(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Zc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=fe(D({},o),{browserUrl:e})),r){let c=D({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(Zn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return He(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Eb),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,u=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":u=D(D({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let m;try{let v=r?r.snapshot:this.routerState.snapshot.root;m=QI(v)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),m=this.currentUrlTree.root}return ZI(m,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=Lo(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Zc,null,i)}navigate(e,i={skipLocationChange:!1}){return JB(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(cr(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=D({},yb):i===!1?r=D({},Xc):r=D(D({},Xc),i),Lo(e))return ab(this.currentUrlTree,e,r);let o=this.parseUrl(e);return ab(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((m,v)=>{s=m,l=v});let u=this.pendingTasks.add();return Mb(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function JB(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new P(4008,!1)}var tH=(()=>{class t{router=d(vn);stateManager=d(Dp);fragment=S("");queryParams=S({});path=S("");serializer=d(il);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof qi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new gn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),sl=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new On("href"),{optional:!0});reactiveHref=wm(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return He(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return He(this._target)}_target=S(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return He(this._queryParams)}_queryParams=S(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return He(this._fragment)}_fragment=S(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return He(this._queryParamsHandling)}_queryParamsHandling=S(void 0);set state(e){this._state.set(e)}get state(){return He(this._state)}_state=S(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return He(this._info)}_info=S(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return He(this._relativeTo)}_relativeTo=S(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return He(this._preserveFragment)}_preserveFragment=S(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return He(this._skipLocationChange)}_skipLocationChange=S(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return He(this._replaceUrl)}_replaceUrl=S(!1);isAnchorElement;onChanges=new k;applicationErrorHandler=d(Zn);options=d(al,{optional:!0});reactiveRouterState=d(tH);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=S(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(Lo(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Rt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:Lo(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return He(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(J(vn),J(Kr),wc("tabindex"),J(Ye),J(G),J(Us))};static \u0275dir=M({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&F("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&de("href",r.reactiveHref(),E_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",W],skipLocationChange:[2,"skipLocationChange","skipLocationChange",W],replaceUrl:[2,"replaceUrl","replaceUrl",W],routerLink:"routerLink"},features:[Pe]})}return t})(),Ib=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new Q;link=d(sl,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(a=>{a instanceof qi&&this.update()})}ngAfterContentInit(){X(this.links.changes,X(null)).pipe(vo()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=st(e).pipe(vo()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=nH(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?D({},yb):D({},Xc);return r=>{let o=r.urlTree;return o?He(bb(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(J(vn),J(G),J(Ye),J(Le))};static \u0275dir=M({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,sl,5),i&2){let a;q(a=Y())&&(r.links=a)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[Pe]})}return t})();function nH(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var iH=new C("");function Tb(t,...n){return fr([{provide:dd,multi:!0,useValue:t},[],{provide:Kr,useFactory:rH},{provide:Ao,multi:!0,useFactory:aH},n.map(e=>e.\u0275providers)])}function rH(){return d(vn).routerState.root}function oH(t,n){return{\u0275kind:t,\u0275providers:n}}function aH(){let t=d(oe);return n=>{let e=t.get(At);if(n!==e.components[0])return;let i=t.get(vn),r=t.get(sH);t.get(lH)===1&&i.initialNavigation(),t.get(cH,null,{optional:!0})?.setUpPreloading(),t.get(iH,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var sH=new C("",{factory:()=>new k}),lH=new C("",{factory:()=>1});var cH=new C("");function Ab(){return oH(8,[wb,{provide:ld,useExisting:wb}])}var kb;try{kb=typeof Intl<"u"&&Intl.v8BreakIterator}catch{kb=!1}var ke=(()=>{class t{_platformId=d(Ur);isBrowser=this._platformId?zm(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||kb)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ob;function ET(){if(Ob==null){let t=typeof document<"u"?document.head:null;Ob=!!(t&&(t.createShadowRoot||t.attachShadow))}return Ob}function Nb(t){if(ET()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Ba(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function qt(t){return t.composedPath?t.composedPath()[0]:t.target}function Pb(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var wp=new WeakMap,_t=(()=>{class t{_appRef;_injector=d(oe);_environmentInjector=d(We);load(e){let i=this._appRef=this._appRef||this._injector.get(At),r=wp.get(i);r||(r={loaders:new Set,refs:[]},wp.set(i,r),i.onDestroy(()=>{wp.get(i)?.refs.forEach(o=>o.destroy()),wp.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Om(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function St(t){return t==null?"":typeof t=="string"?t:`${t}px`}function ll(t){return Array.isArray(t)?t:[t]}function Mr(t,n=0){return ST(t)?Number(t):arguments.length===2?n:0}function ST(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function ti(t){return t instanceof G?t.nativeElement:t}var dH=new C("cdk-dir-doc",{providedIn:"root",factory:()=>d(V)}),uH=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function MT(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?uH.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Mt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=S("ltr");change=new Q;constructor(){let e=d(dH,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(MT(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Yi=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Yi||{}),xp,Ha;function Ep(){if(Ha==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Ha=!1,Ha;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Ha=!0;else{let t=Element.prototype.scrollTo;t?Ha=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Ha=!1}}return Ha}function cl(){if(typeof document!="object"||!document)return Yi.NORMAL;if(xp==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),xp=Yi.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,xp=t.scrollLeft===0?Yi.NEGATED:Yi.INVERTED),t.remove()}return xp}function Sp(t){return t&&typeof t.connect=="function"&&!(t instanceof Ll)}var Qi=(function(t){return t[t.REPLACED=0]="REPLACED",t[t.INSERTED=1]="INSERTED",t[t.MOVED=2]="MOVED",t[t.REMOVED=3]="REMOVED",t})(Qi||{}),Mp=class{viewCacheSize=20;_viewCache=[];applyChanges(n,e,i,r,o){n.forEachOperation((a,s,l)=>{let c,u;if(a.previousIndex==null){let m=()=>i(a,s,l);c=this._insertView(m,l,e,r(a)),u=c?Qi.INSERTED:Qi.REPLACED}else l==null?(this._detachAndCacheView(s,e),u=Qi.REMOVED):(c=this._moveView(s,l,e,r(a)),u=Qi.MOVED);o&&o({context:c?.context,operation:u,record:a})})}detach(){for(let n of this._viewCache)n.destroy();this._viewCache=[]}_insertView(n,e,i,r){let o=this._insertViewFromCache(e,i);if(o){o.context.$implicit=r;return}let a=n();return i.createEmbeddedView(a.templateRef,a.context,a.index)}_detachAndCacheView(n,e){let i=e.detach(n);this._maybeCacheView(i,e)}_moveView(n,e,i,r){let o=i.get(n);return i.move(o,e),o.context.$implicit=r,o}_maybeCacheView(n,e){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(n);else{let i=e.indexOf(n);i===-1?n.destroy():e.remove(i)}}_insertViewFromCache(n,e){let i=this._viewCache.pop();return i&&e.insert(i,n),i||null}};var De=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})();var fH=20,ud=(()=>{class t{_ngZone=d(ie);_platform=d(ke);_renderer=d(Tt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new k;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=fH){return this._platform.isBrowser?new ye(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(ls(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):X()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ie(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=ti(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),IT=(()=>{class t{elementRef=d(G);scrollDispatcher=d(ud);ngZone=d(ie);dir=d(Mt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new k;_renderer=d(Ye);_cleanupScroll;_elementScrolled=new k;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&cl()!=Yi.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),cl()==Yi.INVERTED?e.left=e.right:cl()==Yi.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Ep()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&cl()==Yi.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&cl()==Yi.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),mH=20,Ir=(()=>{class t{_platform=d(ke);_listeners;_viewportSize=null;_change=new k;_document=d(V);constructor(){let e=d(ie),i=d(Tt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=mH){return e>0?this._change.pipe(ls(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var TT=new C("CDK_VIRTUAL_SCROLL_VIEWPORT");var dl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})(),fd=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De,dl,De,dl]})}return t})();var Fb={},Ue=class t{_appId=d(ui);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),Fb.hasOwnProperty(n)||(Fb[n]=0),`${n}${e?t._infix+"-":""}${Fb[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var md=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Xr=class extends md{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},_n=class extends md{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Lb=class extends md{element;constructor(n){super(),this.element=n instanceof G?n.nativeElement:n}},Vo=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Xr)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof _n)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Lb)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},pd=class extends Vo{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(br,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||oe.NULL,o=r.get(We,i.injector);e=Om(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var vi=(()=>{class t extends Vo{_moduleRef=d(br,{optional:!0});_document=d(V);_viewContainerRef=d(ct);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new Q;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[ae]})}return t})(),_i=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})();function Ot(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var AT=Ep();function ml(t){return new Ip(t.get(Ir),t.get(V))}var Ip=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=St(-this._previousScrollPosition.left),n.style.top=St(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),AT&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),AT&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function LT(t,n){return new Tp(t.get(ud),t.get(ie),t.get(Ir),n)}var Tp=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Ie(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var hd=class{enable(){}disable(){}attach(){}};function Vb(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function RT(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function $a(t,n){return new Ap(t.get(ud),t.get(Ir),t.get(ie),n)}var Ap=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Vb(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},VT=(()=>{class t{_injector=d(oe);constructor(){}noop=()=>new hd;close=e=>LT(this._injector,e);block=()=>ml(this._injector);reposition=e=>$a(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Zi=class{positionStrategy;scrollStrategy=new hd;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Rp=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var jT=(()=>{class t{_attachedOverlays=[];_document=d(V);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),BT=(()=>{class t extends jT{_ngZone=d(ie);_renderer=d(Tt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),HT=(()=>{class t extends jT{_platform=d(ke);_ngZone=d(ie);_renderer=d(Tt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=qt(e)};_clickListener=e=>{let i=qt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(kT(s.overlayElement,i)||kT(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function kT(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var UT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Np=(()=>{class t{_platform=d(ke);_containerElement;_document=d(V);_styleLoader=d(_t);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Pb()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Pb()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(UT)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),jb=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Bb(t){return t&&t.nodeType===1}var ul=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new k;_attachments=new k;_detachments=new k;_positionStrategy;_scrollStrategy;_locationChanges=me.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new k;_outsidePointerEvents=new k;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,u=!1,m,v){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=m,this._renderer=v,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=ht(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=D(D({},this._config),n),this._updateElementSize()}setDirection(n){this._config=fe(D({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=St(this._config.width),n.height=St(this._config.height),n.minWidth=St(this._config.minWidth),n.minHeight=St(this._config.minHeight),n.maxWidth=St(this._config.maxWidth),n.maxHeight=St(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Bb(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new jb(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=ll(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=ht(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},OT="cdk-overlay-connected-position-bounding-box",hH=/([A-Za-z%]+)$/;function gd(t,n){return new kp(n,t.get(Ir),t.get(V),t.get(ke),t.get(Np))}var kp=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new k;_resizeSubscription=me.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(OT),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),u=this._getOverlayFit(c,e,i,s);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<u.visibleArea)&&(a={overlayFit:u,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Ua(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(OT),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof G?this._origin.nativeElement:Bb(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=PT(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let u=0-a,m=a+o.width-i.width,v=0-s,_=s+o.height-i.height,b=this._subtractOverflows(o.width,u,m),A=this._subtractOverflows(o.height,v,_),R=b*A;return{visibleArea:R,isCompletelyWithinViewport:o.width*o.height===R,fitsInViewportVertically:A===o.height,fitsInViewportHorizontally:b==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=NT(this._overlayRef.getConfig().minHeight),s=NT(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=PT(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,m=0;return r.width<=o.width?u=c||-a:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?m=l||-s:m=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:m},{x:n.x+u,y:n.y+m}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!gH(this._lastScrollVisibility,i)){let r=new Rp(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),b=this._lastBoundingBoxSize.height;o=_*2,a=n.y-_,o>b&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-b/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,m,v;if(c)v=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)m=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),b=this._lastBoundingBoxSize.width;u=_*2,m=n.x-_,u>b&&!this._isInitialRender&&!this._growAfterOpen&&(m=n.x-b/2)}return{top:a,left:m,bottom:s,right:v,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=St(i.width),r.height=St(i.height),r.top=St(i.top)||"auto",r.bottom=St(i.bottom)||"auto",r.left=St(i.left)||"auto",r.right=St(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=St(o)),a&&(r.maxWidth=St(a))}this._lastBoundingBoxSize=i,Ua(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Ua(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Ua(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();Ua(i,this._getExactOverlayY(e,n,u)),Ua(i,this._getExactOverlayX(e,n,u))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=St(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=St(a.maxWidth):o&&(i.maxWidth="")),Ua(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=St(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=St(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:RT(n,i),isOriginOutsideView:Vb(n,i),isOverlayClipped:RT(e,i),isOverlayOutsideView:Vb(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&ll(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof G)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function Ua(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function NT(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(hH);return!e||e==="px"?parseFloat(n):null}return t||null}function PT(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function gH(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var FT="cdk-global-overlay-wrapper";function jo(t){return new Op}var Op=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(FT),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),u=this._xPosition,m=this._xOffset,v=this._overlayRef.getConfig().direction==="rtl",_="",b="",A="";l?A="flex-start":u==="center"?(A="center",v?b=m:_=m):v?u==="left"||u==="end"?(A="flex-end",_=m):(u==="right"||u==="start")&&(A="flex-start",b=m):u==="left"||u==="start"?(A="flex-start",_=m):(u==="right"||u==="end")&&(A="flex-end",b=m),n.position=this._cssPosition,n.marginLeft=l?"0":_,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":b,e.justifyContent=A,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(FT),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},zT=(()=>{class t{_injector=d(oe);constructor(){}global(){return jo()}flexibleConnectedTo(e){return gd(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),vd=new C("OVERLAY_DEFAULT_CONFIG");function Jr(t,n){t.get(_t).load(UT);let e=t.get(Np),i=t.get(V),r=t.get(Ue),o=t.get(At),a=t.get(Mt),s=t.get(Ye,null,{optional:!0})||t.get(Tt).createRenderer(null,null),l=new Zi(n),c=t.get(vd,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),m=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),m.appendChild(u),l.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let v=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Bb(v)?v.after(m):v?.type==="parent"?v.element.appendChild(m):e.getContainerElement().appendChild(m),new ul(new pd(u,o,t),m,u,l,t.get(ie),t.get(BT),i,t.get(No),t.get(HT),n?.disableAnimations??t.get(xc,null,{optional:!0})==="NoopAnimations",t.get(We),s)}var $T=(()=>{class t{scrollStrategies=d(VT);_positionBuilder=d(zT);_injector=d(oe);constructor(){}create(e){return Jr(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),vH=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],_H=new C("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>$a(t)}}),fl=(()=>{class t{elementRef=d(G);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),GT=new C("cdk-connected-overlay-default-config"),Pp=(()=>{class t{_dir=d(Mt,{optional:!0});_injector=d(oe);_overlayRef;_templatePortal;_backdropSubscription=me.EMPTY;_attachSubscription=me.EMPTY;_detachSubscription=me.EMPTY;_positionSubscription=me.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(_H);_ngZone=d(ie);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new Q;positionChange=new Q;attach=new Q;detach=new Q;overlayKeydown=new Q;overlayOutsideClick=new Q;constructor(){let e=d(je),i=d(ct),r=d(GT,{optional:!0}),o=d(vd,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new _n(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=vH);let e=this._overlayRef=Jr(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ot(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=qt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Zi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=gd(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof fl?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof fl?this.origin.elementRef.nativeElement:this.origin instanceof G?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(tg(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",W],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",W],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",W],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",W],push:[2,"cdkConnectedOverlayPush","push",W],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",W],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",W],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Pe]})}return t})(),Tr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({providers:[$T],imports:[De,_i,fd,fd]})}return t})();function Ga(t){return t.buttons===0||t.detail===0}function Wa(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var _d;function WT(){if(_d==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>_d=!0}))}finally{_d=_d||!1}return _d}function pl(t){return WT()?t:!!t.capture}var qT=new C("cdk-input-modality-detector-options"),YT={ignoreKeys:[18,17,224,91,16]},QT=650,Hb={passive:!0,capture:!0},ZT=(()=>{class t{_platform=d(ke);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Ct(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=qt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<QT||(this._modality.next(Ga(e)?"keyboard":"mouse"),this._mostRecentTarget=qt(e))};_onTouchstart=e=>{if(Wa(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=qt(e)};constructor(){let e=d(ie),i=d(V),r=d(qT,{optional:!0});if(this._options=D(D({},YT),r),this.modalityDetected=this._modality.pipe(Wl(1)),this.modalityChanged=this.modalityDetected.pipe(Fu()),this._platform.isBrowser){let o=d(Tt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Hb),o.listen(i,"mousedown",this._onMousedown,Hb),o.listen(i,"touchstart",this._onTouchstart,Hb)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),yd=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(yd||{}),KT=new C("cdk-focus-monitor-default-options"),Fp=pl({passive:!0,capture:!0}),Fn=(()=>{class t{_ngZone=d(ie);_platform=d(ke);_inputModalityDetector=d(ZT);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(V);_stopInputModalityDetector=new k;constructor(){let e=d(KT,{optional:!0});this._detectionMode=e?.detectionMode||yd.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=qt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=ti(e);if(!this._platform.isBrowser||r.nodeType!==1)return X();let o=Nb(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new k,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=ti(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=ti(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===yd.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===yd.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?QT:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=qt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Fp),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Fp)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Te(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Fp),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Fp),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var hl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),Lp;function yH(){if(Lp===void 0&&(Lp=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Lp=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Lp}function qa(t){return yH()?.createHTML(t)||t}function XT(t,n,e){let i=e.sanitize(Ft.HTML,n);t.innerHTML=qa(i||"")}var JT=new Set,Ya,Vp=(()=>{class t{_platform=d(ke);_nonce=d(Ta,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):CH}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&bH(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function bH(t,n){if(!JT.has(t))try{Ya||(Ya=document.createElement("style"),n&&Ya.setAttribute("nonce",n),Ya.setAttribute("type","text/css"),document.head.appendChild(Ya)),Ya.sheet&&(Ya.sheet.insertRule(`@media ${t} {body{ }}`,0),JT.add(t))}catch(e){console.error(e)}}function CH(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var bd=(()=>{class t{_mediaMatcher=d(Vp);_zone=d(ie);_queries=new Map;_destroySubject=new k;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return eA(ll(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=eA(ll(e)).map(a=>this._registerQuery(a).observable),o=sr(r);return o=_o(o.pipe(lt(1)),o.pipe(Wl(1),$l(0))),o.pipe(pe(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ye(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(qe(i),pe(({matches:a})=>({query:e,matches:a})),Te(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function eA(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var DH=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var jp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({providers:[DH]})}return t})();var $b=(()=>{class t{_platform=d(ke);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return xH(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=wH(kH(e));if(i&&(tA(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=tA(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!AH(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return RH(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function wH(t){try{return t.frameElement}catch{return null}}function xH(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function EH(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function SH(t){return IH(t)&&t.type=="hidden"}function MH(t){return TH(t)&&t.hasAttribute("href")}function IH(t){return t.nodeName.toLowerCase()=="input"}function TH(t){return t.nodeName.toLowerCase()=="a"}function rA(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function tA(t){if(!rA(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function AH(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function RH(t){return SH(t)?!1:EH(t)||MH(t)||t.hasAttribute("contenteditable")||rA(t)}function kH(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var zb=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?ht(n,{injector:this._injector}):setTimeout(n)}},Gb=(()=>{class t{_checker=d($b);_ngZone=d(ie);_document=d(V);_injector=d(oe);constructor(){d(_t).load(hl)}create(e,i=!1){return new zb(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var oA=new C("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),aA=new C("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),OH=0,Cd=(()=>{class t{_ngZone=d(ie);_defaultOptions=d(aA,{optional:!0});_liveElement;_document=d(V);_sanitizer=d(Wc);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(oA,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:XT(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${OH++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Bo=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(Bo||{}),nA="cdk-high-contrast-black-on-white",iA="cdk-high-contrast-white-on-black",Ub="cdk-high-contrast-active",sA=(()=>{class t{_platform=d(ke);_hasCheckedHighContrastMode=!1;_document=d(V);_breakpointSubscription;constructor(){this._breakpointSubscription=d(bd).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return Bo.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return Bo.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return Bo.BLACK_ON_WHITE}return Bo.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(Ub,nA,iA),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===Bo.BLACK_ON_WHITE?e.add(Ub,nA):i===Bo.WHITE_ON_BLACK&&e.add(Ub,iA)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Wb=(()=>{class t{constructor(){d(sA)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[jp]})}return t})();function NH(t,n){}var Ho=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var Yb=(()=>{class t extends Vo{_elementRef=d(G);_focusTrapFactory=d(Gb);_config;_interactivityChecker=d($b);_ngZone=d(ie);_focusMonitor=d(Fn);_renderer=d(Ye);_changeDetectorRef=d(Le);_injector=d(oe);_platform=d(ke);_document=d(V);_portalOutlet;_focusTrapped=new k;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(Ho,{optional:!0})||new Ho,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||ht(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Ba(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Ba();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Ba()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&dt(vi,7),i&2){let o;q(o=Y())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&de("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[ae],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&N(0,NH,0,0,"ng-template",0)},dependencies:[vi],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),Dd=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new k;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Ot(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},PH=new C("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>ml(t)}}),FH=new C("DialogData"),LH=new C("DefaultDialogConfig");function VH(t){let n=S(t),e=new Q;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var Qb=(()=>{class t{_injector=d(oe);_defaultOptions=d(LH,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Np);_idGenerator=d(Ue);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new k;_afterOpenedAtThisLevel=new k;_ariaHiddenElements=new Map;_scrollStrategy=d(PH);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=Ai(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(qe(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new Ho;i=D(D({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=Jr(this._injector,o),s=new Dd(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(lt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){qb(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){qb(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),qb(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Zi({positionStrategy:e.positionStrategy||jo().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:Ho,useValue:r},{provide:Dd,useValue:i},{provide:ul,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=Yb;let l=new Xr(s,r.viewContainerRef,oe.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof je){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=D(D({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new _n(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new Xr(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:FH,useValue:e.data},{provide:Dd,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(Mt,null,{optional:!0}))&&s.push({provide:Mt,useValue:VH(e.direction)}),oe.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qb(t,n){let e=t.length;for(;e--;)n(t[e])}var lA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({providers:[Qb],imports:[Tr,_i,Wb,_i]})}return t})();function eo(t){return t!=null&&`${t}`!="false"}var cA={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var jH=new C("MATERIAL_ANIMATIONS"),dA=null;function BH(){return d(jH,{optional:!0})?.animationsDisabled||d(xc,{optional:!0})==="NoopAnimations"?"di-disabled":(dA??=d(Vp).matchMedia("(prefers-reduced-motion)").matches,dA?"reduced-motion":"enabled")}function tt(){return BH()!=="enabled"}var HH=200,Bp=class{_letterKeyStream=new k;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new k;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:HH;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(jt(e=>this._pressedLetters.push(e)),$l(n),Ie(()=>this._pressedLetters.length>0),pe(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var gl=class{_items;_activeItemIndex=S(-1);_activeItem=S(null);_wrap=!1;_typeaheadSubscription=me.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Jt?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Cr(n)&&(this._effectRef=Vi(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new k;change=new k;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Bp(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||Ot(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Cr(this._items)?this._items():this._items instanceof Jt?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Md=class extends gl{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var to=class extends gl{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var fA=" ";function mA(t,n,e){let i=pA(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(fA)))}function Zb(t,n,e){let i=pA(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(fA)):t.removeAttribute(n)}function pA(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}function UH(t,n){}var vl=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Kb="mdc-dialog--open",hA="mdc-dialog--opening",gA="mdc-dialog--closing",zH=150,$H=75,GH=(()=>{class t extends Yb{_animationStateChanged=new Q;_animationsEnabled=!tt();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?_A(this._config.enterAnimationDuration)??zH:0;_exitAnimationDuration=this._animationsEnabled?_A(this._config.exitAnimationDuration)??$H:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(vA,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(hA,Kb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Kb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Kb),this._animationsEnabled?(this._hostElement.style.setProperty(vA,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(gA)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(hA,gA)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Et("id",r._config.id),de("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),K("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[ae],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",0)(1,"div",1),N(2,UH,0,0,"ng-template",2),p()())},dependencies:[vi],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),vA="--mat-dialog-transition-duration";function _A(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Mr(t.substring(0,t.length-2)):t.endsWith("s")?Mr(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Hp=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Hp||{}),Vt=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new po(1);_beforeClosed=new po(1);_result;_closeFallbackTimeout;_state=Hp.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(Ie(r=>r.state==="opened"),lt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ie(r=>r.state==="closed"),lt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),sn(this.backdropClick(),this.keydownEvents().pipe(Ie(r=>r.keyCode===27&&!this.disableClose&&!Ot(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),WH(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(Ie(i=>i.state==="closing"),lt(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Hp.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Hp.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function WH(t,n,e){return t._closeInteractionType=n,t.close(e)}var yi=new C("MatMdcDialogData"),Xb=new C("mat-mdc-dialog-default-options"),qH=new C("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>ml(t)}}),Ki=(()=>{class t{_defaultOptions=d(Xb,{optional:!0});_scrollStrategy=d(qH);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ue);_injector=d(oe);_dialog=d(Qb);_animationsDisabled=tt();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new k;_afterOpenedAtThisLevel=new k;dialogConfigClass=vl;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=Ai(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(qe(void 0)));constructor(){this._dialogRefConstructor=Vt,this._dialogContainerType=GH,this._dialogDataToken=yi}open(e,i){let r;i=D(D({},this._defaultOptions||new vl),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,fe(D({},i),{positionStrategy:jo(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:Ho,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var rn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[J_([IT])]})}return t})();var Jb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({providers:[Ki],imports:[lA,Tr,_i,De]})}return t})();var MA=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(J(Ye),J(G))};static \u0275dir=M({type:t})}return t})(),YH=(()=>{class t extends MA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,features:[ae]})}return t})(),Fd=new C("");var QH={provide:Fd,useExisting:dn(()=>Ln),multi:!0};function ZH(){let t=pi()?pi().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var KH=new C(""),Ln=(()=>{class t extends MA{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!ZH())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(J(Ye),J(G),J(KH,8))};static \u0275dir=M({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&F("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[Ce([QH]),ae]})}return t})();function i0(t){return t==null||r0(t)===0}function r0(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var zo=new C(""),Xp=new C(""),XH=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,re=class{static min(n){return JH(n)}static max(n){return eU(n)}static required(n){return IA(n)}static requiredTrue(n){return tU(n)}static email(n){return nU(n)}static minLength(n){return iU(n)}static maxLength(n){return TA(n)}static pattern(n){return rU(n)}static nullValidator(n){return zp()}static compose(n){return PA(n)}static composeAsync(n){return FA(n)}};function JH(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function eU(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function IA(t){return i0(t.value)?{required:!0}:null}function tU(t){return t.value===!0?null:{required:!0}}function nU(t){return i0(t.value)||XH.test(t.value)?null:{email:!0}}function iU(t){return n=>{let e=n.value?.length??r0(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function TA(t){return n=>{let e=n.value?.length??r0(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function rU(t){if(!t)return zp;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(i0(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function zp(t){return null}function AA(t){return t!=null}function RA(t){return To(t)?st(t):t}function kA(t){let n={};return t.forEach(e=>{n=e!=null?D(D({},n),e):n}),Object.keys(n).length===0?null:n}function OA(t,n){return n.map(e=>e(t))}function oU(t){return!t.validate}function NA(t){return t.map(n=>oU(n)?n:e=>n.validate(e))}function PA(t){if(!t)return null;let n=t.filter(AA);return n.length==0?null:function(e){return kA(OA(e,n))}}function o0(t){return t!=null?PA(NA(t)):null}function FA(t){if(!t)return null;let n=t.filter(AA);return n.length==0?null:function(e){let i=OA(e,n).map(RA);return zl(i).pipe(pe(kA))}}function a0(t){return t!=null?FA(NA(t)):null}function yA(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function LA(t){return t._rawValidators}function VA(t){return t._rawAsyncValidators}function e0(t){return t?Array.isArray(t)?t:[t]:[]}function $p(t,n){return Array.isArray(t)?t.includes(n):t===n}function bA(t,n){let e=e0(n);return e0(t).forEach(r=>{$p(e,r)||e.push(r)}),e}function CA(t,n){return e0(n).filter(e=>!$p(t,e))}var Gp=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=o0(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=a0(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Xi=class extends Gp{name;get formDirective(){return null}get path(){return null}},Ji=class extends Gp{_parent=null;name=null;valueAccessor=null},Wp=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Vn=(()=>{class t extends Wp{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(J(Ji,2))};static \u0275dir=M({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&K("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[ae]})}return t})(),bi=(()=>{class t extends Wp{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(J(Xi,10))};static \u0275dir=M({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&K("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[ae]})}return t})();var Td="VALID",Up="INVALID",_l="PENDING",Ad="DISABLED",Uo=class{},qp=class extends Uo{value;source;constructor(n,e){super(),this.value=n,this.source=e}},kd=class extends Uo{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Od=class extends Uo{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},yl=class extends Uo{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Yp=class extends Uo{source;constructor(n){super(),this.source=n}},Pd=class extends Uo{source;constructor(n){super(),this.source=n}};function s0(t){return(Jp(t)?t.validators:t)||null}function aU(t){return Array.isArray(t)?o0(t):t||null}function l0(t,n){return(Jp(n)?n.asyncValidators:t)||null}function sU(t){return Array.isArray(t)?a0(t):t||null}function Jp(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function jA(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new P(1e3,"");if(!i[e])throw new P(1001,"")}function BA(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new P(-1002,"")})}var bl=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return He(this.statusReactive)}set status(n){He(()=>this.statusReactive.set(n))}_status=Rt(()=>this.statusReactive());statusReactive=S(void 0);get valid(){return this.status===Td}get invalid(){return this.status===Up}get pending(){return this.status===_l}get disabled(){return this.status===Ad}get enabled(){return this.status!==Ad}errors;get pristine(){return He(this.pristineReactive)}set pristine(n){He(()=>this.pristineReactive.set(n))}_pristine=Rt(()=>this.pristineReactive());pristineReactive=S(!0);get dirty(){return!this.pristine}get touched(){return He(this.touchedReactive)}set touched(n){He(()=>this.touchedReactive.set(n))}_touched=Rt(()=>this.touchedReactive());touchedReactive=S(!1);get untouched(){return!this.touched}_events=new k;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(bA(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(bA(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(CA(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(CA(n,this._rawAsyncValidators))}hasValidator(n){return $p(this._rawValidators,n)}hasAsyncValidator(n){return $p(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(fe(D({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Od(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new Od(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(fe(D({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new kd(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new kd(!0,i))}markAsPending(n={}){this.status=_l;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new yl(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(fe(D({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Ad,this.errors=null,this._forEachChild(r=>{r.disable(fe(D({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new qp(this.value,i)),this._events.next(new yl(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(fe(D({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Td,this._forEachChild(i=>{i.enable(fe(D({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(fe(D({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Td||this.status===_l)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new qp(this.value,e)),this._events.next(new yl(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(fe(D({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ad:Td}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=_l,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=RA(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new yl(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new Q,this.statusChanges=new Q}_calculateStatus(){return this._allControlsDisabled()?Ad:this.errors?Up:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(_l)?_l:this._anyControlsHaveStatus(Up)?Up:Td}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new kd(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Od(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Jp(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=aU(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=sU(this._rawAsyncValidators)}},Cl=class extends bl{constructor(n,e,i){super(s0(e),l0(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){BA(this,!0,n),Object.keys(n).forEach(i=>{jA(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,fe(D({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Pd(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var t0=class extends Cl{};var Ld=new C("",{factory:()=>eh}),eh="always";function HA(t,n){return[...n.path,t]}function Qp(t,n,e=eh){c0(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),cU(t,n),uU(t,n),dU(t,n),lU(t,n)}function DA(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Kp(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Zp(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function lU(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function c0(t,n){let e=LA(t);n.validator!==null?t.setValidators(yA(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=VA(t);n.asyncValidator!==null?t.setAsyncValidators(yA(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Zp(n._rawValidators,r),Zp(n._rawAsyncValidators,r)}function Kp(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=LA(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=VA(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Zp(n._rawValidators,i),Zp(n._rawAsyncValidators,i),e}function cU(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&UA(t,n)})}function dU(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&UA(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function UA(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function uU(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function zA(t,n){t==null,c0(t,n)}function fU(t,n){return Kp(t,n)}function $A(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function mU(t){return Object.getPrototypeOf(t.constructor)===YH}function GA(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function WA(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Ln?e=o:mU(o)?i=o:r=o}),r||i||e||null}function pU(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var hU={provide:Xi,useExisting:dn(()=>Vd)},Rd=Promise.resolve(),Vd=(()=>{class t extends Xi{callSetDisabledState;get submitted(){return He(this.submittedReactive)}_submitted=Rt(()=>this.submittedReactive());submittedReactive=S(!1);_directives=new Set;form;ngSubmit=new Q;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Cl({},o0(e),a0(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Rd.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),Qp(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Rd.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Rd.then(()=>{let i=this._findContainer(e.path),r=new Cl({});zA(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Rd.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Rd.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),GA(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Yp(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(J(zo,10),J(Xp,10),J(Ld,8))};static \u0275dir=M({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&F("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ce([hU]),ae]})}return t})();function wA(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function xA(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var Nd=class extends bl{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(s0(e),l0(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Jp(e)&&(e.nonNullable||e.initialValueIsDefault)&&(xA(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Pd(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){wA(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){wA(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){xA(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var gU=t=>t instanceof Nd;var vU={provide:Ji,useExisting:dn(()=>d0)},EA=Promise.resolve(),d0=(()=>{class t extends Ji{_changeDetectorRef;callSetDisabledState;control=new Nd;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Q;constructor(e,i,r,o,a,s){super(),this._changeDetectorRef=a,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=WA(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),$A(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){Qp(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){EA.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&W(i);EA.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?HA(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(J(Xi,9),J(zo,10),J(Xp,10),J(Fd,10),J(Le,8),J(Ld,8))};static \u0275dir=M({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Ce([vU]),ae,Pe]})}return t})();var Ci=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var n0=class extends bl{constructor(n,e,i){super(s0(e),l0(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){BA(this,!1,n),n.forEach((i,r)=>{jA(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],fe(D({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Pd(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var _U=(()=>{class t extends Xi{callSetDisabledState;get submitted(){return He(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Rt(()=>this._submittedReactive());_submittedReactive=S(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Kp(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return Qp(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){DA(e.control||null,e,!1),pU(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,GA(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Yp(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(DA(i||null,e),gU(r)&&(Qp(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);zA(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&fU(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){c0(this.form,this),this._oldForm&&Kp(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(J(zo,10),J(Xp,10),J(Ld,8))};static \u0275dir=M({type:t,features:[ae,Pe]})}return t})();var qA=new C("");var yU={provide:Ji,useExisting:dn(()=>ni)},ni=(()=>{class t extends Ji{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new Q;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=WA(this,o)}ngOnChanges(e){this._added||this._setUpControl(),$A(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return HA(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(J(Xi,13),J(zo,10),J(Xp,10),J(Fd,10),J(qA,8))};static \u0275dir=M({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[Ce([yU]),ae,Pe]})}return t})();var bU={provide:Xi,useExisting:dn(()=>Yt)},Yt=(()=>{class t extends _U{form=null;ngSubmit=new Q;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&F("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ce([bU]),ae]})}return t})();function CU(t){return typeof t=="number"?t:parseInt(t,10)}var YA=(()=>{class t{_validator=zp;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):zp,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,features:[Pe]})}return t})();var DU={provide:zo,useExisting:dn(()=>ii),multi:!0};var ii=(()=>{class t extends YA{required;inputName="required";normalizeInput=W;createValidator=e=>IA;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&de("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Ce([DU]),ae]})}return t})();var wU={provide:zo,useExisting:dn(()=>no),multi:!0},no=(()=>{class t extends YA{maxlength;inputName="maxlength";normalizeInput=e=>CU(e);createValidator=e=>TA(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","maxlength","","formControlName",""],["","maxlength","","formControl",""],["","maxlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&de("maxlength",r._enabled?r.maxlength:null)},inputs:{maxlength:"maxlength"},standalone:!1,features:[Ce([wU]),ae]})}return t})();var QA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})();function SA(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var xU=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return SA(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Cl(r,o)}record(e,i=null){let r=this._reduceControls(e);return new t0(r,i)}control(e,i,r){let o={};return this.useNonNullable?(SA(i)?o=i:(o.validators=i,o.asyncValidators=r),new Nd(e,fe(D({},o),{nonNullable:!0}))):new Nd(e,i,r)}array(e,i,r){let o=e.map(a=>this._createControl(a));return new n0(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof Nd)return e;if(e instanceof bl)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var th=(()=>{class t extends xU{group(e,i=null){return super.group(e,i)}control(e,i,r){return super.control(e,i,r)}array(e,i,r){return super.array(e,i,r)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ZA=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Ld,useValue:e.callSetDisabledState??eh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[QA]})}return t})(),u0=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:qA,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:Ld,useValue:e.callSetDisabledState??eh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[QA]})}return t})();var KA=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=eo(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=eo(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(de("aria-orientation",r.vertical?"vertical":"horizontal"),K("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return t})(),io=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var wl,XA=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function f0(){if(wl)return wl;if(typeof document!="object"||!document)return wl=new Set(XA),wl;let t=document.createElement("input");return wl=new Set(XA.filter(n=>(t.setAttribute("type",n),t.type===n))),wl}var Di=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Di||{}),m0=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Di.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},JA=pl({passive:!0,capture:!0}),p0=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,JA)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,JA)))}_delegateEventHandler=n=>{let e=qt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},jd={enterDuration:225,exitDuration:150},SU=800,eR=pl({passive:!0,capture:!0}),tR=["mousedown","touchstart"],nR=["mouseup","mouseleave","touchend","touchcancel"],MU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),Bd=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new p0;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=ti(i)),o&&o.get(_t).load(MU)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=D(D({},jd),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||IU(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${s-a}px`,u.style.top=`${l-a}px`,u.style.height=`${a*2}px`,u.style.width=`${a*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let m=window.getComputedStyle(u),v=m.transitionProperty,_=m.transitionDuration,b=v==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,A=new m0(this,u,i,b);u.style.transform="scale3d(1, 1, 1)",A.state=Di.FADING_IN,i.persistent||(this._mostRecentTransientRipple=A);let R=null;return!b&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let L=()=>{R&&(R.fallbackTimer=null),clearTimeout(yt),this._finishRippleTransition(A)},_e=()=>this._destroyRipple(A),yt=setTimeout(_e,c+100);u.addEventListener("transitionend",L),u.addEventListener("transitioncancel",_e),R={onTransitionEnd:L,onTransitionCancel:_e,fallbackTimer:yt}}),this._activeRipples.set(A,R),(b||!c)&&this._finishRippleTransition(A),A}fadeOutRipple(n){if(n.state===Di.FADING_OUT||n.state===Di.HIDDEN)return;let e=n.element,i=D(D({},jd),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Di.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=ti(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,tR.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{nR.forEach(e=>{this._triggerElement.addEventListener(e,this,eR)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Di.FADING_IN?this._startFadeOutTransition(n):n.state===Di.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Di.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Di.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Ga(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+SU;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Wa(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Di.VISIBLE||n.config.terminateOnPointerUp&&n.state===Di.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(tR.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(nR.forEach(e=>n.removeEventListener(e,this,eR)),this._pointerUpEventsRegistered=!1))}};function IU(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var h0=new C("mat-ripple-global-options"),$o=(()=>{class t{_elementRef=d(G);_animationsDisabled=tt();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(ie),i=d(ke),r=d(h0,{optional:!0}),o=d(oe);this._globalOptions=r||{},this._rippleRenderer=new Bd(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:D(D(D({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,D(D({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,D(D({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&K("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var TU={capture:!0},AU=["focus","mousedown","mouseenter","touchstart"],g0="mat-ripple-loader-uninitialized",v0="mat-ripple-loader-class-name",iR="mat-ripple-loader-centered",nh="mat-ripple-loader-disabled",rR=(()=>{class t{_document=d(V);_animationsDisabled=tt();_globalRippleOptions=d(h0,{optional:!0});_platform=d(ke);_ngZone=d(ie);_injector=d(oe);_eventCleanups;_hosts=new Map;constructor(){let e=d(Tt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>AU.map(i=>e.listen(this._document,i,this._onInteraction,TU)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(g0,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(v0))&&e.setAttribute(v0,i.className||""),i.centered&&e.setAttribute(iR,""),i.disabled&&e.setAttribute(nh,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(nh,""):e.removeAttribute(nh)}_onInteraction=e=>{let i=qt(e);if(i instanceof HTMLElement){let r=i.closest(`[${g0}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(v0)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??jd.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??jd.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(nh),rippleConfig:{centered:e.hasAttribute(iR),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Bd(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(g0)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var RU=["mat-icon-button",""],kU=["*"],OU=new C("MAT_BUTTON_CONFIG");function oR(t){return t==null?void 0:$i(t)}var ih=(()=>{class t{_elementRef=d(G);_ngZone=d(ie);_animationsDisabled=tt();_config=d(OU,{optional:!0});_focusMonitor=d(Fn);_cleanupClick;_renderer=d(Ye);_rippleLoader=d(rR);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(_t).load(wi);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(de("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Gt(r.color?"mat-"+r.color:""),K("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",W],disabled:[2,"disabled","disabled",W],ariaDisabled:[2,"aria-disabled","ariaDisabled",W],disabledInteractive:[2,"disabledInteractive","disabledInteractive",W],tabIndex:[2,"tabIndex","tabIndex",oR],_tabindex:[2,"tabindex","_tabindex",oR]}})}return t})(),ro=(()=>{class t extends ih{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[ae],attrs:RU,ngContentSelectors:kU,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(),en(0,"span",0),Z(1),en(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var Go=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var NU=["matButton",""],sR=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],lR=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var PU=["mat-mini-fab",""],FU=`.mat-mdc-fab-base {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 56px;
  height: 56px;
  padding: 0;
  border: none;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  -moz-appearance: none;
  -webkit-appearance: none;
  overflow: visible;
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1), opacity 15ms linear 30ms, transform 270ms 0ms cubic-bezier(0, 0, 0.2, 1);
  flex-shrink: 0;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-fab-base .mat-mdc-button-ripple,
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple,
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-fab-base .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-fab-base .mdc-button__label,
.mat-mdc-fab-base .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-fab-base .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-mdc-fab-base:focus-visible > .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-fab-base._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-fab-base::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mat-mdc-fab-base[hidden] {
  display: none;
}
.mat-mdc-fab-base::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mat-mdc-fab-base:active, .mat-mdc-fab-base:focus {
  outline: none;
}
.mat-mdc-fab-base:hover {
  cursor: pointer;
}
.mat-mdc-fab-base > svg {
  width: 100%;
}
.mat-mdc-fab-base .mat-icon, .mat-mdc-fab-base .material-icons {
  transition: transform 180ms 90ms cubic-bezier(0, 0, 0.2, 1);
  fill: currentColor;
  will-change: transform;
}
.mat-mdc-fab-base .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}
.mat-mdc-fab-base[disabled], .mat-mdc-fab-base.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-fab-base[disabled], .mat-mdc-fab-base[disabled]:focus, .mat-mdc-fab-base.mat-mdc-button-disabled, .mat-mdc-fab-base.mat-mdc-button-disabled:focus {
  box-shadow: none;
}
.mat-mdc-fab-base.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-fab {
  background-color: var(--mat-fab-container-color, var(--mat-sys-primary-container));
  border-radius: var(--mat-fab-container-shape, var(--mat-sys-corner-large));
  color: var(--mat-fab-foreground-color, var(--mat-sys-on-primary-container, inherit));
  box-shadow: var(--mat-fab-container-elevation-shadow, var(--mat-sys-level3));
}
@media (hover: hover) {
  .mat-mdc-fab:hover {
    box-shadow: var(--mat-fab-hover-container-elevation-shadow, var(--mat-sys-level4));
  }
}
.mat-mdc-fab:focus {
  box-shadow: var(--mat-fab-focus-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-fab:active, .mat-mdc-fab:focus:active {
  box-shadow: var(--mat-fab-pressed-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-fab[disabled], .mat-mdc-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-fab-disabled-state-foreground-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-fab-disabled-state-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-fab .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-fab-touch-target-size, 48px);
  display: var(--mat-fab-touch-target-display, block);
  left: 50%;
  width: var(--mat-fab-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-fab .mat-ripple-element {
  background-color: var(--mat-fab-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-fab .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-state-layer-color, var(--mat-sys-on-primary-container));
}
.mat-mdc-fab.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-disabled-state-layer-color);
}
.mat-mdc-fab:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-fab.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-fab.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-fab.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-fab:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}

.mat-mdc-mini-fab {
  width: 40px;
  height: 40px;
  background-color: var(--mat-fab-small-container-color, var(--mat-sys-primary-container));
  border-radius: var(--mat-fab-small-container-shape, var(--mat-sys-corner-medium));
  color: var(--mat-fab-small-foreground-color, var(--mat-sys-on-primary-container, inherit));
  box-shadow: var(--mat-fab-small-container-elevation-shadow, var(--mat-sys-level3));
}
@media (hover: hover) {
  .mat-mdc-mini-fab:hover {
    box-shadow: var(--mat-fab-small-hover-container-elevation-shadow, var(--mat-sys-level4));
  }
}
.mat-mdc-mini-fab:focus {
  box-shadow: var(--mat-fab-small-focus-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-mini-fab:active, .mat-mdc-mini-fab:focus:active {
  box-shadow: var(--mat-fab-small-pressed-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-mini-fab[disabled], .mat-mdc-mini-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-fab-small-disabled-state-foreground-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-fab-small-disabled-state-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-mini-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-mini-fab .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-fab-small-touch-target-size, 48px);
  display: var(--mat-fab-small-touch-target-display);
  left: 50%;
  width: var(--mat-fab-small-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-mini-fab .mat-ripple-element {
  background-color: var(--mat-fab-small-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-mini-fab .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-small-state-layer-color, var(--mat-sys-on-primary-container));
}
.mat-mdc-mini-fab.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-small-disabled-state-layer-color);
}
.mat-mdc-mini-fab:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-small-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-mini-fab.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-mini-fab.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-mini-fab.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-small-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-mini-fab:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-small-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}

.mat-mdc-extended-fab {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  padding-left: 20px;
  padding-right: 20px;
  width: auto;
  max-width: 100%;
  line-height: normal;
  box-shadow: var(--mat-fab-extended-container-elevation-shadow, var(--mat-sys-level3));
  height: var(--mat-fab-extended-container-height, 56px);
  border-radius: var(--mat-fab-extended-container-shape, var(--mat-sys-corner-large));
  font-family: var(--mat-fab-extended-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-fab-extended-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-fab-extended-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-fab-extended-label-text-tracking, var(--mat-sys-label-large-tracking));
}
@media (hover: hover) {
  .mat-mdc-extended-fab:hover {
    box-shadow: var(--mat-fab-extended-hover-container-elevation-shadow, var(--mat-sys-level4));
  }
}
.mat-mdc-extended-fab:focus {
  box-shadow: var(--mat-fab-extended-focus-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-extended-fab:active, .mat-mdc-extended-fab:focus:active {
  box-shadow: var(--mat-fab-extended-pressed-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-extended-fab[disabled], .mat-mdc-extended-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-extended-fab[disabled], .mat-mdc-extended-fab[disabled]:focus, .mat-mdc-extended-fab.mat-mdc-button-disabled, .mat-mdc-extended-fab.mat-mdc-button-disabled:focus {
  box-shadow: none;
}
.mat-mdc-extended-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
[dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .mat-icon, [dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .material-icons,
.mat-mdc-extended-fab > .mat-icon,
.mat-mdc-extended-fab > .material-icons {
  margin-left: -8px;
  margin-right: 12px;
}
.mat-mdc-extended-fab .mdc-button__label + .mat-icon,
.mat-mdc-extended-fab .mdc-button__label + .material-icons, [dir=rtl] .mat-mdc-extended-fab > .mat-icon, [dir=rtl] .mat-mdc-extended-fab > .material-icons {
  margin-left: 12px;
  margin-right: -8px;
}
.mat-mdc-extended-fab .mat-mdc-button-touch-target {
  width: 100%;
}
`,aR=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),ut=(()=>{class t extends ih{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=LU(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?aR.get(this._appearance):null,o=aR.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[ae],attrs:NU,ngContentSelectors:lR,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(sR),en(0,"span",0),Z(1),xt(2,"span",1),Z(3,1),Lt(),Z(4,2),en(5,"span",2)(6,"span",3)),i&2&&K("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function LU(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var VU=new C("mat-mdc-fab-default-options",{providedIn:"root",factory:()=>_0}),_0={color:"accent"};var rh=(()=>{class t extends ih{_options=d(VU,{optional:!0});_isFab=!0;constructor(){super(),this._options=this._options||_0,this.color=this._options.color||_0.color}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-mini-fab",""],["a","mat-mini-fab",""],["button","matMiniFab",""],["a","matMiniFab",""]],hostAttrs:[1,"mdc-fab","mat-mdc-fab-base","mdc-fab--mini","mat-mdc-mini-fab"],exportAs:["matButton","matAnchor"],features:[ae],attrs:PU,ngContentSelectors:lR,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(sR),en(0,"span",0),Z(1),xt(2,"span",1),Z(3,1),Lt(),Z(4,2),en(5,"span",2)(6,"span",3)),i&2&&K("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[FU],encapsulation:2,changeDetection:0})}return t})();var Wo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[Go,De]})}return t})();function jU(t,n){if(t&1){let e=ze();f(0,"div",1)(1,"button",2),F("click",function(){Ae(e);let r=y();return Re(r.action())}),g(2),p()()}if(t&2){let e=y();h(2),z(" ",e.data.action," ")}}var BU=["label"];function HU(t,n){}var UU=Math.pow(2,31)-1,Hd=class{_overlayRef;instance;containerInstance;_afterDismissed=new k;_afterOpened=new k;_onAction=new k;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,UU))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},cR=new C("MatSnackBarData"),qo=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},zU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),$U=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),GU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),WU=(()=>{class t{snackBarRef=d(Hd);data=d(cR);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(f(0,"div",0),g(1),p(),I(2,jU,3,1,"div",1)),i&2&&(h(),z(" ",r.data.message,`
`),h(),T(r.hasAction?2:-1))},dependencies:[ut,zU,$U,GU],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),y0="_mat-snack-bar-enter",b0="_mat-snack-bar-exit",qU=(()=>{class t extends Vo{_ngZone=d(ie);_elementRef=d(G);_changeDetectorRef=d(Le);_platform=d(ke);_animationsDisabled=tt();snackBarConfig=d(qo);_document=d(V);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(oe);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new k;_onExit=new k;_onEnter=new k;_animationState="void";_live;_label;_role;_liveElementId=d(Ue).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===b0?this._completeExit():e===y0&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?ht(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(y0)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(y0)},200)))}exit(){return this._destroyed?X(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?ht(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(b0)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(b0),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&dt(vi,7)(BU,7),i&2){let o;q(o=Y())&&(r._portalOutlet=o.first),q(o=Y())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&F("animationend",function(a){return r.onAnimationEnd(a.animationName)})("animationcancel",function(a){return r.onAnimationEnd(a.animationName)}),i&2&&K("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[ae],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2,0)(3,"div",3),N(4,HU,0,0,"ng-template",4),p(),U(5,"div"),p()()),i&2&&(h(5),de("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[vi],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return t})(),YU=new C("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new qo}),Qt=(()=>{class t{_live=d(Cd);_injector=d(oe);_breakpointObserver=d(bd);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(YU);_animationsDisabled=tt();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=WU;snackBarContainerComponent=qU;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=D(D({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=oe.create({parent:r||this._injector,providers:[{provide:qo,useValue:i}]}),a=new Xr(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=D(D(D({},new qo),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new Hd(a,o);if(e instanceof je){let l=new _n(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(l)}else{let l=this._createInjector(r,s),c=new Xr(e,void 0,l),u=a.attachComponentPortal(c);s.instance=u.instance}return this._breakpointObserver.observe(cA.HandsetPortrait).pipe(Te(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Zi;i.direction=e.direction;let r=jo(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Jr(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return oe.create({parent:r||this._injector,providers:[{provide:Hd,useValue:i},{provide:cR,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var dR=(()=>{class t{_animationsDisabled=tt();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&K("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var QU=["text"],ZU=[[["mat-icon"]],"*"],KU=["mat-icon","*"];function XU(t,n){if(t&1&&U(0,"mat-pseudo-checkbox",1),t&2){let e=y();x("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function JU(t,n){if(t&1&&U(0,"mat-pseudo-checkbox",3),t&2){let e=y();x("disabled",e.disabled)}}function ez(t,n){if(t&1&&(f(0,"span",4),g(1),p()),t&2){let e=y();h(),z("(",e.group.label,")")}}var ah=new C("MAT_OPTION_PARENT_COMPONENT"),sh=new C("MatOptgroup");var oh=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},yn=(()=>{class t{_element=d(G);_changeDetectorRef=d(Le);_parent=d(ah,{optional:!0});group=d(sh,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ue).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=S(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new Q;_text;_stateChanges=new k;constructor(){let e=d(_t);e.load(wi),e.load(hl),this._signalDisableRipple=!!this._parent&&Cr(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ot(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new oh(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&dt(QU,7),i&2){let o;q(o=Y())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&F("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(Et("id",r.id),de("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),K("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",W]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:KU,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Me(ZU),I(0,XU,1,2,"mat-pseudo-checkbox",1),Z(1),f(2,"span",2,0),Z(4,1),p(),I(5,JU,1,1,"mat-pseudo-checkbox",3),I(6,ez,2,1,"span",4),U(7,"div",5)),i&2&&(T(r.multiple?0:-1),h(5),T(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),h(),T(r.group&&r.group._inert?6:-1),h(),x("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[dR,$o],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function C0(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function D0(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var w0=class{_box;_destroyed=new k;_resizeSubject=new k;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ye(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ie(e=>e.some(i=>i.target===n)),ju({bufferSize:1,refCount:!0}),Te(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},uR=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(ie);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new w0(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var tz=["notch"],nz=["matFormFieldNotchedOutline",""],iz=["*"],fR=["iconPrefixContainer"],mR=["textPrefixContainer"],pR=["iconSuffixContainer"],hR=["textSuffixContainer"],rz=["textField"],oz=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],az=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function sz(t,n){t&1&&U(0,"span",21)}function lz(t,n){if(t&1&&(f(0,"label",20),Z(1,1),I(2,sz,1,0,"span",21),p()),t&2){let e=y(2);x("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),de("for",e._control.disableAutomaticLabeling?null:e._control.id),h(2),T(!e.hideRequiredMarker&&e._control.required?2:-1)}}function cz(t,n){if(t&1&&I(0,lz,3,5,"label",20),t&2){let e=y();T(e._hasFloatingLabel()?0:-1)}}function dz(t,n){t&1&&U(0,"div",7)}function uz(t,n){}function fz(t,n){if(t&1&&N(0,uz,0,0,"ng-template",13),t&2){y(2);let e=$t(1);x("ngTemplateOutlet",e)}}function mz(t,n){if(t&1&&(f(0,"div",9),I(1,fz,1,1,null,13),p()),t&2){let e=y();x("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),h(),T(e._forceDisplayInfixLabel()?-1:1)}}function pz(t,n){t&1&&(f(0,"div",10,2),Z(2,2),p())}function hz(t,n){t&1&&(f(0,"div",11,3),Z(2,3),p())}function gz(t,n){}function vz(t,n){if(t&1&&N(0,gz,0,0,"ng-template",13),t&2){y();let e=$t(1);x("ngTemplateOutlet",e)}}function _z(t,n){t&1&&(f(0,"div",14,4),Z(2,4),p())}function yz(t,n){t&1&&(f(0,"div",15,5),Z(2,5),p())}function bz(t,n){t&1&&U(0,"div",16)}function Cz(t,n){t&1&&(f(0,"div",18),Z(1,6),p())}function Dz(t,n){if(t&1&&(f(0,"mat-hint",22),g(1),p()),t&2){let e=y(2);x("id",e._hintLabelId),h(),ve(e.hintLabel)}}function wz(t,n){if(t&1&&(f(0,"div",19),I(1,Dz,2,2,"mat-hint",22),Z(2,7),U(3,"div",23),Z(4,8),p()),t&2){let e=y();h(),T(e.hintLabel?1:-1)}}var on=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-label"]]})}return t})(),DR=new C("MatError"),Yo=(()=>{class t{id=d(Ue).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&Et("id",r.id)},inputs:{id:"id"},features:[Ce([{provide:DR,useExisting:t}])]})}return t})(),jn=(()=>{class t{align="start";id=d(Ue).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Et("id",r.id),de("align",null),K("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),xz=new C("MatPrefix");var Ez=new C("MatSuffix");var wR=new C("FloatingLabelParent"),gR=(()=>{class t{_elementRef=d(G);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(uR);_ngZone=d(ie);_parent=d(wR);_resizeSubscription=new me;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return Sz(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&K("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function Sz(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var vR="mdc-line-ripple--active",lh="mdc-line-ripple--deactivating",_R=(()=>{class t{_elementRef=d(G);_cleanupTransitionEnd;constructor(){let e=d(ie),i=d(Ye);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(lh),e.add(vR)}deactivate(){this._elementRef.nativeElement.classList.add(lh)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(lh);e.propertyName==="opacity"&&r&&i.remove(vR,lh)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),yR=(()=>{class t{_elementRef=d(G);_ngZone=d(ie);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&dt(tz,5),i&2){let o;q(o=Y())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&K("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:nz,ngContentSelectors:iz,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Me(),en(0,"div",1),xt(1,"div",2,0),Z(3),Lt(),en(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),Ud=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t})}return t})();var zd=new C("MatFormField"),Mz=new C("MAT_FORM_FIELD_DEFAULT_OPTIONS"),bR="fill",Iz="auto",CR="fixed",Tz="translateY(-50%)",bn=(()=>{class t{_elementRef=d(G);_changeDetectorRef=d(Le);_platform=d(ke);_idGenerator=d(Ue);_ngZone=d(ie);_defaults=d(Mz,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Pc("iconPrefixContainer");_textPrefixContainerSignal=Pc("textPrefixContainer");_iconSuffixContainerSignal=Pc("iconSuffixContainer");_textSuffixContainerSignal=Pc("textSuffixContainer");_prefixSuffixContainers=Rt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=SM(on);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=eo(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||Iz}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||bR;this._appearanceSignal.set(i)}_appearanceSignal=S(bR);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||CR}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||CR}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new k;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=tt();constructor(){let e=this._defaults,i=d(Mt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Vi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Rt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(qe([void 0,void 0]),pe(()=>[i.errorState,i.userAriaDescribedBy]),Vu(),Ie(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Te(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),sn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){RM({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Rt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",m=`${a+s}px`,_=`calc(${u} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,b=`var(--mat-mdc-form-field-label-transform, ${Tz} translateX(${_}))`,A=a+s+l+c;return[b,A]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(_m(o,r._labelChild,on,5),rt(o,Ud,5)(o,xz,5)(o,Ez,5)(o,DR,5)(o,jn,5)),i&2){bm();let a;q(a=Y())&&(r._formFieldControl=a.first),q(a=Y())&&(r._prefixChildren=a),q(a=Y())&&(r._suffixChildren=a),q(a=Y())&&(r._errorChildren=a),q(a=Y())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(ym(r._iconPrefixContainerSignal,fR,5)(r._textPrefixContainerSignal,mR,5)(r._iconSuffixContainerSignal,pR,5)(r._textSuffixContainerSignal,hR,5),dt(rz,5)(fR,5)(mR,5)(pR,5)(hR,5)(gR,5)(yR,5)(_R,5)),i&2){bm(4);let o;q(o=Y())&&(r._textField=o.first),q(o=Y())&&(r._iconPrefixContainer=o.first),q(o=Y())&&(r._textPrefixContainer=o.first),q(o=Y())&&(r._iconSuffixContainer=o.first),q(o=Y())&&(r._textSuffixContainer=o.first),q(o=Y())&&(r._floatingLabel=o.first),q(o=Y())&&(r._notchedOutline=o.first),q(o=Y())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&K("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Ce([{provide:zd,useExisting:t},{provide:wR,useExisting:t}])],ngContentSelectors:az,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Me(oz),N(0,cz,1,1,"ng-template",null,0,Bs),f(2,"div",6,1),F("click",function(a){return r._control.onContainerClick(a)}),I(4,dz,1,0,"div",7),f(5,"div",8),I(6,mz,2,2,"div",9),I(7,pz,3,0,"div",10),I(8,hz,3,0,"div",11),f(9,"div",12),I(10,vz,1,1,null,13),Z(11),p(),I(12,_z,3,0,"div",14),I(13,yz,3,0,"div",15),p(),I(14,bz,1,0,"div",16),p(),f(15,"div",17),I(16,Cz,2,0,"div",18)(17,wz,5,1,"div",19),p()),i&2){let o;h(2),K("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),h(2),T(!r._hasOutline()&&!r._control.disabled?4:-1),h(2),T(r._hasOutline()?6:-1),h(),T(r._hasIconPrefix?7:-1),h(),T(r._hasTextPrefix?8:-1),h(2),T(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),h(2),T(r._hasTextSuffix?12:-1),h(),T(r._hasIconSuffix?13:-1),h(),T(r._hasOutline()?-1:14),h(),K("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();h(),T((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[gR,yR,zs,_R,jn],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var xR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var x0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[Go,xR,yn,De]})}return t})();var $d=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new k;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Gd=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var dh=class{applyChanges(n,e,i,r,o){n.forEachOperation((a,s,l)=>{let c,u;if(a.previousIndex==null){let m=i(a,s,l);c=e.createEmbeddedView(m.templateRef,m.context,m.index),u=Qi.INSERTED}else l==null?(e.remove(s),u=Qi.REMOVED):(c=e.get(s),e.move(c,l),u=Qi.MOVED);o&&o({context:c?.context,operation:u,record:a})})}detach(){}};var Az=["*"];var Rz=new C("MAT_CARD_CONFIG"),er=(()=>{class t{appearance;constructor(){let e=d(Rz,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&K("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:Az,decls:1,vars:0,template:function(i,r){i&1&&(Me(),Z(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})(),tr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var nr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var E0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var kz=["mat-internal-form-field",""],Oz=["*"],ER=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&K("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:kz,ngContentSelectors:Oz,decls:1,vars:0,template:function(i,r){i&1&&(Me(),Z(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var Qo=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var xl=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var S0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var SR=new C("MAT_INPUT_VALUE_ACCESSOR");var M0=new C("CdkAccordion"),MR=(()=>{class t{_stateChanges=new k;_openCloseAllActions=new k;id=d(Ue).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",W]},exportAs:["cdkAccordion"],features:[Ce([{provide:M0,useExisting:t}]),Pe]})}return t})(),IR=(()=>{class t{accordion=d(M0,{optional:!0,skipSelf:!0});_changeDetectorRef=d(Le);_expansionDispatcher=d(Gd);_openCloseAllSubscription=me.EMPTY;closed=new Q;opened=new Q;destroyed=new Q;expandedChange=new Q;id=d(Ue).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=S(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",W],disabled:[2,"disabled","disabled",W]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[Ce([{provide:M0,useValue:void 0}])]})}return t})(),TR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})();var Nz=["body"],Pz=["bodyWrapper"],Fz=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],Lz=["mat-expansion-panel-header","*","mat-action-row"];function Vz(t,n){}var jz=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],Bz=["mat-panel-title","mat-panel-description","*"];function Hz(t,n){t&1&&(xt(0,"span",1),gr(),xt(1,"svg",2),en(2,"path",3),Lt()())}var I0=new C("MAT_ACCORDION"),AR=new C("MAT_EXPANSION_PANEL"),Uz=(()=>{class t{_template=d(je);_expansionPanel=d(AR,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),RR=new C("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),ri=(()=>{class t extends IR{_viewContainerRef=d(ct);_animationsDisabled=tt();_document=d(V);_ngZone=d(ie);_elementRef=d(G);_renderer=d(Ye);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new Q;afterCollapse=new Q;_inputChanges=new k;accordion=d(I0,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=d(Ue).getId("mat-expansion-panel-header-");constructor(){super();let e=d(RR,{optional:!0});this._expansionDispatcher=d(Gd),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(qe(null),Ie(()=>this.expanded&&!this._portal),lt(1)).subscribe(()=>{this._portal=new _n(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&rt(o,Uz,5),i&2){let a;q(a=Y())&&(r._lazyContent=a.first)}},viewQuery:function(i,r){if(i&1&&dt(Nz,5)(Pz,5),i&2){let o;q(o=Y())&&(r._body=o.first),q(o=Y())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&K("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",W],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[Ce([{provide:I0,useValue:void 0},{provide:AR,useExisting:t}]),ae,Pe],ngContentSelectors:Lz,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Me(Fz),Z(0),f(1,"div",2,0)(3,"div",3,1)(5,"div",4),Z(6,1),N(7,Vz,0,0,"ng-template",5),p(),Z(8,2),p()()),i&2&&(h(),de("inert",r.expanded?null:""),h(2),x("id",r.id),de("aria-labelledby",r._headerId),h(4),x("cdkPortalOutlet",r._portal))},dependencies:[vi],styles:[`.mat-expansion-panel {
  box-sizing: content-box;
  display: block;
  margin: 0;
  overflow: hidden;
}
.mat-expansion-panel.mat-expansion-panel-animations-enabled {
  transition: margin 225ms cubic-bezier(0.4, 0, 0.2, 1), box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel {
  position: relative;
  background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  color: var(--mat-expansion-container-text-color, var(--mat-sys-on-surface));
  border-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-expansion-panel:not([class*=mat-elevation-z]) {
  box-shadow: var(--mat-expansion-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}
.mat-accordion .mat-expansion-panel:not(.mat-expanded), .mat-accordion .mat-expansion-panel:not(.mat-expansion-panel-spacing) {
  border-radius: 0;
}
.mat-accordion .mat-expansion-panel:first-of-type {
  border-top-right-radius: var(--mat-expansion-container-shape, 12px);
  border-top-left-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-accordion .mat-expansion-panel:last-of-type {
  border-bottom-right-radius: var(--mat-expansion-container-shape, 12px);
  border-bottom-left-radius: var(--mat-expansion-container-shape, 12px);
}
@media (forced-colors: active) {
  .mat-expansion-panel {
    outline: solid 1px;
  }
}

.mat-expansion-panel-content-wrapper {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content-wrapper {
  transition: grid-template-rows 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
  grid-template-rows: 1fr;
}
@supports not (grid-template-rows: 0fr) {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}
@media print {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}

.mat-expansion-panel-content {
  display: flex;
  flex-direction: column;
  overflow: visible;
  min-height: 0;
  visibility: hidden;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content {
  transition: visibility 190ms linear;
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper > .mat-expansion-panel-content {
  visibility: visible;
}
.mat-expansion-panel-content {
  font-family: var(--mat-expansion-container-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-expansion-container-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-expansion-container-text-weight, var(--mat-sys-body-large-weight));
  line-height: var(--mat-expansion-container-text-line-height, var(--mat-sys-body-large-line-height));
  letter-spacing: var(--mat-expansion-container-text-tracking, var(--mat-sys-body-large-tracking));
}

.mat-expansion-panel-body {
  padding: 0 24px 16px;
}

.mat-expansion-panel-spacing {
  margin: 16px 0;
}
.mat-accordion > .mat-expansion-panel-spacing:first-child, .mat-accordion > *:first-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-top: 0;
}
.mat-accordion > .mat-expansion-panel-spacing:last-child, .mat-accordion > *:last-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-bottom: 0;
}

.mat-action-row {
  border-top-style: solid;
  border-top-width: 1px;
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  padding: 16px 8px 16px 24px;
  border-top-color: var(--mat-expansion-actions-divider-color, var(--mat-sys-outline));
}
.mat-action-row .mat-button-base,
.mat-action-row .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-action-row .mat-button-base,
[dir=rtl] .mat-action-row .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}
`],encapsulation:2,changeDetection:0})}return t})(),kR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-action-row"]],hostAttrs:[1,"mat-action-row"]})}return t})(),oi=(()=>{class t{panel=d(ri,{host:!0});_element=d(G);_focusMonitor=d(Fn);_changeDetectorRef=d(Le);_parentChangeSubscription=me.EMPTY;constructor(){d(_t).load(wi);let e=this.panel,i=d(RR,{optional:!0}),r=d(new On("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(Ie(a=>!!(a.hideToggle||a.togglePosition))):ft;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=sn(e.opened,e.closed,o,e._inputChanges.pipe(Ie(a=>!!(a.hideToggle||a.disabled||a.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(Ie(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:Ot(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&F("click",function(){return r._toggle()})("keydown",function(a){return r._keydown(a)}),i&2&&(de("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),E("height",r._getHeaderHeight()),K("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:$i(e)]},ngContentSelectors:Bz,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(Me(jz),xt(0,"span",0),Z(1),Z(2,1),Z(3,2),Lt(),I(4,Hz,3,0,"span",1)),i&2&&(K("mat-content-hide-toggle",!r._showToggle()),h(4),T(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
  display: flex;
  flex-direction: row;
  align-items: center;
  padding: 0 24px;
  border-radius: inherit;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-header {
  transition: height 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header::before {
  border-radius: inherit;
}
.mat-expansion-panel-header {
  height: var(--mat-expansion-header-collapsed-state-height, 48px);
  font-family: var(--mat-expansion-header-text-font, var(--mat-sys-title-medium-font));
  font-size: var(--mat-expansion-header-text-size, var(--mat-sys-title-medium-size));
  font-weight: var(--mat-expansion-header-text-weight, var(--mat-sys-title-medium-weight));
  line-height: var(--mat-expansion-header-text-line-height, var(--mat-sys-title-medium-line-height));
  letter-spacing: var(--mat-expansion-header-text-tracking, var(--mat-sys-title-medium-tracking));
}
.mat-expansion-panel-header.mat-expanded {
  height: var(--mat-expansion-header-expanded-state-height, 64px);
}
.mat-expansion-panel-header[aria-disabled=true] {
  color: var(--mat-expansion-header-disabled-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-expansion-panel-header:not([aria-disabled=true]) {
  cursor: pointer;
}
.mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
  background: var(--mat-expansion-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
@media (hover: none) {
  .mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
    background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  }
}
.mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-keyboard-focused, .mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-program-focused {
  background: var(--mat-expansion-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
.mat-expansion-panel-header._mat-animation-noopable {
  transition: none;
}
.mat-expansion-panel-header:focus, .mat-expansion-panel-header:hover {
  outline: none;
}
.mat-expansion-panel-header.mat-expanded:focus, .mat-expansion-panel-header.mat-expanded:hover {
  background: inherit;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before {
  flex-direction: row-reverse;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 16px 0 0;
}
[dir=rtl] .mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 0 0 16px;
}

.mat-content {
  display: flex;
  flex: 1;
  flex-direction: row;
  overflow: hidden;
}
.mat-content.mat-content-hide-toggle {
  margin-right: 8px;
}
[dir=rtl] .mat-content.mat-content-hide-toggle {
  margin-right: 0;
  margin-left: 8px;
}
.mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-left: 24px;
  margin-right: 0;
}
[dir=rtl] .mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-right: 24px;
  margin-left: 0;
}

.mat-expansion-panel-header-title {
  color: var(--mat-expansion-header-text-color, var(--mat-sys-on-surface));
}

.mat-expansion-panel-header-title,
.mat-expansion-panel-header-description {
  display: flex;
  flex-grow: 1;
  flex-basis: 0;
  margin-right: 16px;
  align-items: center;
}
[dir=rtl] .mat-expansion-panel-header-title,
[dir=rtl] .mat-expansion-panel-header-description {
  margin-right: 0;
  margin-left: 16px;
}
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-title,
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-description {
  color: inherit;
}

.mat-expansion-panel-header-description {
  flex-grow: 2;
  color: var(--mat-expansion-header-description-color, var(--mat-sys-on-surface-variant));
}

.mat-expansion-panel-animations-enabled .mat-expansion-indicator {
  transition: transform 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header.mat-expanded .mat-expansion-indicator {
  transform: rotate(180deg);
}
.mat-expansion-indicator::after {
  border-style: solid;
  border-width: 0 2px 2px 0;
  content: "";
  padding: 3px;
  transform: rotate(45deg);
  vertical-align: middle;
  color: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-legacy-header-indicator-display, none);
}
.mat-expansion-indicator svg {
  width: 24px;
  height: 24px;
  margin: 0 -8px;
  vertical-align: middle;
  fill: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-header-indicator-display, inline-block);
}

@media (forced-colors: active) {
  .mat-expansion-panel-content {
    border-top: 1px solid;
    border-top-left-radius: 0;
    border-top-right-radius: 0;
  }
}
`],encapsulation:2,changeDetection:0})}return t})(),Ar=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-panel-description"]],hostAttrs:[1,"mat-expansion-panel-header-description"]})}return t})(),xi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),Rr=(()=>{class t extends MR{_keyManager;_ownHeaders=new Jt;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(qe(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new to(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&rt(o,oi,5),i&2){let a;q(a=Y())&&(r._headers=a)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&K("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",W],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[Ce([{provide:I0,useExisting:t}]),ae]})}return t})(),uh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[TR,_i,De]})}return t})();var Zo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[jp,bn,De]})}return t})();var fh=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,e){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=e.map(i=>this._trackTile(i))}_trackTile(n){let e=this._findMatchingGap(n.colspan);return this._markTilePosition(e,n),this.columnIndex=e+n.colspan,new T0(this.rowIndex,e)}_findMatchingGap(n){n>this.tracker.length;let e=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}if(e=this.tracker.indexOf(0,this.columnIndex),e==-1){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}i=this._findGapEndIndex(e),this.columnIndex=e+1}while(i-e<n||i==0);return Math.max(e,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let e=n+1;e<this.tracker.length;e++)if(this.tracker[e]!=0)return e;return this.tracker.length}_markTilePosition(n,e){for(let i=0;i<e.colspan;i++)this.tracker[n+i]=e.rowspan}},T0=class{row;col;constructor(n,e){this.row=n,this.col=e}};var OR=["*"];var Gz=`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`,NR=new C("MAT_GRID_LIST"),Wz=(()=>{class t{_element=d(G);_gridList=d(NR,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(e){this._rowspan=Math.round(Mr(e))}get colspan(){return this._colspan}set colspan(e){this._colspan=Math.round(Mr(e))}_setStyle(e,i){this._element.nativeElement.style[e]=i}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&de("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:OR,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(Me(),xt(0,"div",0),Z(1),Lt())},styles:[`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var qz=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,qd=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,e,i,r){this._gutterSize=PR(n),this._rows=e.rowCount,this._rowspan=e.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,e){return`(${n}% - (${this._gutterSize} * ${e}))`}getTilePosition(n,e){return e===0?"0":Za(`(${n} + ${this._gutterSize}) * ${e}`)}getTileSize(n,e){return`(${n} * ${e}) + (${e-1} * ${this._gutterSize})`}setStyle(n,e,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,e,r,o)}setColStyles(n,e,i,r){let o=this.getBaseTileSize(i,r),a=this._direction==="rtl"?"right":"left";n._setStyle(a,this.getTilePosition(o,e)),n._setStyle("width",Za(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},A0=class extends qd{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,e,i,r){super.init(n,e,i,r),this.fixedRowHeight=PR(this.fixedRowHeight),qz.test(this.fixedRowHeight)}setRowStyles(n,e){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,e)),n._setStyle("height",Za(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",Za(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}},R0=class extends qd{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,e,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,e)),n._setStyle("paddingTop",Za(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",Za(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(e=>{e._setStyle("marginTop",null),e._setStyle("paddingTop",null)})}_parseRatio(n){let e=n.split(":");e.length,this.rowHeightRatio=parseFloat(e[0])/parseFloat(e[1])}},k0=class extends qd{setRowStyles(n,e){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,e)),n._setStyle("height",Za(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}};function Za(t){return`calc(${t})`}function PR(t){return t.match(/([A-Za-z%]+)$/)?t:`${t}px`}var Yz="fit",ir=(()=>{class t{_element=d(G);_dir=d(Mt,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(e){this._cols=Math.max(1,Math.round(Mr(e)))}get gutterSize(){return this._gutter}set gutterSize(e){this._gutter=`${e??""}`}get rowHeight(){return this._rowHeight}set rowHeight(e){let i=`${e??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(e){this._tileStyler&&this._tileStyler.reset(this),e===Yz?this._tileStyler=new k0:e&&e.indexOf(":")>-1?this._tileStyler=new R0(e):this._tileStyler=new A0(e)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new fh);let e=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,e,this.cols,r),i.forEach((o,a)=>{let s=e.positions[a];this._tileStyler.setStyle(o,s.row,s.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(e){e&&(this._element.nativeElement.style[e[0]]=e[1])}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&rt(o,Wz,5),i&2){let a;q(a=Y())&&(r._tiles=a)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&de("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[Ce([{provide:NR,useExisting:t}])],ngContentSelectors:OR,decls:2,vars:0,template:function(i,r){i&1&&(Me(),xt(0,"div"),Z(1),Lt())},styles:[Gz],encapsulation:2,changeDetection:0})}return t})(),O0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[S0,De,S0]})}return t})();function FR(t){return Error(`Unable to find icon with the name "${t}"`)}function Qz(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function LR(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function VR(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var so=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},BR=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new so(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(Ft.HTML,r);if(!a)throw VR(r);let s=qa(a);return this._addSvgIconConfig(e,i,new so("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new so(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Ft.HTML,i);if(!o)throw VR(i);let a=qa(o);return this._addSvgIconSetConfig(e,new so("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Ft.RESOURCE_URL,e);if(!i)throw LR(e);let r=this._cachedIconsByUrl.get(i);return r?X(mh(r)):this._loadSvgIconFromConfig(new so(e,null)).pipe(jt(o=>this._cachedIconsByUrl.set(i,o)),pe(o=>mh(o)))}getNamedSvgIcon(e,i=""){let r=jR(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):Ul(FR(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?X(mh(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(pe(i=>mh(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return X(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(yo(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(Ft.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),X(null)})));return zl(o).pipe(pe(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw FR(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(jt(i=>e.svgText=i),pe(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?X(null):this._fetchIcon(e).pipe(jt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(qa("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(qa("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw Qz();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(Ft.RESOURCE_URL,i);if(!a)throw LR(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(pe(c=>qa(c)),ua(()=>this._inProgressUrlFetches.delete(a)),Gl());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(jR(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return Zz(o)?new so(o.url,null,o.options):new so(o,null)}}static \u0275fac=function(i){return new(i||t)(le(Xy,8),le(Wc),le(V,8),le(cn))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function mh(t){return t.cloneNode(!0)}function jR(t,n){return t+":"+n}function Zz(t){return!!(t.url&&t.options)}var Kz=["*"],Xz=new C("MAT_ICON_DEFAULT_OPTIONS"),Jz=new C("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(V),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),HR=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],e$=HR.map(t=>`[${t}]`).join(", "),t$=/^url\(['"]?#(.*?)['"]?\)$/,Bn=(()=>{class t{_elementRef=d(G);_iconRegistry=d(BR);_location=d(Jz);_errorHandler=d(cn);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=me.EMPTY;constructor(){let e=d(new On("aria-hidden"),{optional:!0}),i=d(Xz,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(e$),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)HR.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(t$):null;if(c){let u=r.get(s);u||(u=[],r.set(s,u)),u.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(lt(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(de("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),Gt(r.color?"mat-"+r.color:""),K("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",W],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:Kz,decls:1,vars:0,template:function(i,r){i&1&&(Me(),Z(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Ko=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var n$=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return t})(),i$={passive:!0},UR=(()=>{class t{_platform=d(ke);_ngZone=d(ie);_renderer=d(Tt).createRenderer(null,null);_styleLoader=d(_t);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return ft;this._styleLoader.load(n$);let i=ti(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new k,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,i$)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=ti(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var zR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({})}return t})();var r$=["button","checkbox","file","hidden","image","radio","range","reset","submit"],o$=new C("MAT_INPUT_CONFIG"),Ei=(()=>{class t{_elementRef=d(G);_platform=d(ke);ngControl=d(Ji,{optional:!0,self:!0});_autofillMonitor=d(UR);_ngZone=d(ie);_formField=d(zd,{optional:!0});_renderer=d(Ye);_uid=d(Ue).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(o$,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new k;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=eo(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(re.required)??!1}set required(e){this._required=eo(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&f0().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=eo(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>f0().has(e));constructor(){let e=d(Vd,{optional:!0}),i=d(Yt,{optional:!0}),r=d(Qo),o=d(SR,{optional:!0,self:!0}),a=this._elementRef.nativeElement,s=a.nodeName.toLowerCase();o?Cr(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new xl(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=s==="select",this._isTextarea=s==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Vi(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){r$.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&F("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Et("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),de("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),K("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",W]},exportAs:["matInput"],features:[Ce([{provide:Ud,useExisting:t}]),Pe]})}return t})(),N0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[Zo,Zo,zR,De]})}return t})();var a$=["mat-menu-item",""],s$=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],l$=["mat-icon, [matMenuItemIcon]","*"];function c$(t,n){t&1&&(gr(),f(0,"svg",2),U(1,"polygon",3),p())}var d$=["*"];function u$(t,n){if(t&1){let e=ze();xt(0,"div",0),vm("click",function(){Ae(e);let r=y();return Re(r.closed.emit("click"))})("animationstart",function(r){Ae(e);let o=y();return Re(o._onAnimationStart(r.animationName))})("animationend",function(r){Ae(e);let o=y();return Re(o._onAnimationDone(r.animationName))})("animationcancel",function(r){Ae(e);let o=y();return Re(o._onAnimationDone(r.animationName))}),xt(1,"div",1),Z(2),Lt()()}if(t&2){let e=y();Gt(e._classList),K("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Et("id",e.panelId),de("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var F0=new C("MAT_MENU_PANEL"),Zd=(()=>{class t{_elementRef=d(G);_document=d(V);_focusMonitor=d(Fn);_parentMenu=d(F0,{optional:!0});_changeDetectorRef=d(Le);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new k;_focused=new k;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(_t).load(wi),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&F("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(de("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),K("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",W],disableRipple:[2,"disableRipple","disableRipple",W]},exportAs:["matMenuItem"],attrs:a$,ngContentSelectors:l$,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Me(s$),Z(0),f(1,"span",0),Z(2,1),p(),U(3,"div",1),I(4,c$,2,0,":svg:svg",2)),i&2&&(h(3),x("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),h(),T(r._triggersSubmenu?4:-1))},dependencies:[$o],encapsulation:2,changeDetection:0})}return t})();var f$=new C("MatMenuContent");var m$=new C("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),P0="_mat-menu-enter",ph="_mat-menu-exit",Il=(()=>{class t{_elementRef=d(G);_changeDetectorRef=d(Le);_injector=d(oe);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=tt();_allItems;_directDescendantItems=new Jt;_classList={};_panelAnimationState="void";_animationDone=new k;_isAnimating=S(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=D({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new Q;close=this.closed;panelId=d(Ue).getId("mat-menu-panel-");constructor(){let e=d(m$);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new to(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(qe(this._directDescendantItems),mt(e=>sn(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(qe(this._directDescendantItems),mt(i=>sn(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Ot(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=ht(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=fe(D({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===ph;(i||e===P0)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===P0||e===ph)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(ph),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?P0:ph)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(qe(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&rt(o,f$,5)(o,Zd,5)(o,Zd,4),i&2){let a;q(a=Y())&&(r.lazyContent=a.first),q(a=Y())&&(r._allItems=a),q(a=Y())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&dt(je,5),i&2){let o;q(o=Y())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&de("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",W],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:W(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Ce([{provide:F0,useExisting:t}])],ngContentSelectors:d$,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Me(),Fs(0,u$,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),p$=new C("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>$a(t)}});var Ml=new WeakMap,h$=(()=>{class t{_canHaveBackdrop;_element=d(G);_viewContainerRef=d(ct);_menuItemInstance=d(Zd,{optional:!0,self:!0});_dir=d(Mt,{optional:!0});_focusMonitor=d(Fn);_ngZone=d(ie);_injector=d(oe);_scrollStrategy=d(p$);_changeDetectorRef=d(Le);_animationsDisabled=tt();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=me.EMPTY;_menuCloseSubscription=me.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(F0,{optional:!0});this._parentMaterialMenu=i instanceof Il?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Ml.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Ml.get(i);Ml.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Il&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(Te(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Il&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(lt(1)).subscribe(()=>{i.detach(),Ml.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Ml.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Jr(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Il&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Zi({positionStrategy:gd(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[a,s],[u,m]=[r,o],v=0;if(this._triggersSubmenu()){if(m=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}v=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=a==="top"?"bottom":"top",c=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:v},{originX:o,originY:l,overlayX:m,overlayY:a,offsetY:v},{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:-v},{originX:o,originY:c,overlayX:m,overlayY:s,offsetY:-v}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:X(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(Ie(a=>this._menuOpen&&a!==this._menuItemInstance)):X();return sn(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new _n(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Ml.get(e)===this}_triggerIsAriaDisabled(){return W(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){fm()};static \u0275dir=M({type:t})}return t})(),$R=(()=>{class t extends h${_cleanupTouchstart;_hoverSubscription=me.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new Q;onMenuOpen=this.menuOpened;menuClosed=new Q;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(Ye);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{Wa(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){Ga(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&F("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&de("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[ae]})}return t})();var GR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[Go,Tr,De,dl]})}return t})();var v$=["trigger"],_$=["panel"],y$=[[["mat-select-trigger"]],"*"],b$=["mat-select-trigger","*"];function C$(t,n){if(t&1&&(f(0,"span",4),g(1),p()),t&2){let e=y();h(),ve(e.placeholder)}}function D$(t,n){t&1&&Z(0)}function w$(t,n){if(t&1&&(f(0,"span",11),g(1),p()),t&2){let e=y(2);h(),ve(e.triggerValue)}}function x$(t,n){if(t&1&&(f(0,"span",5),I(1,D$,1,0)(2,w$,2,1,"span",11),p()),t&2){let e=y();h(),T(e.customTrigger?1:2)}}function E$(t,n){if(t&1){let e=ze();f(0,"div",12,1),F("keydown",function(r){Ae(e);let o=y();return Re(o._handleKeydown(r))}),Z(2,1),p()}if(t&2){let e=y();Gt(e.panelClass),K("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),de("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var S$=new C("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>$a(t)}}),M$=new C("MAT_SELECT_CONFIG"),I$=new C("MatSelectTrigger"),L0=class{source;value;constructor(n,e){this.source=n,this.value=e}},kr=(()=>{class t{_viewportRuler=d(Ir);_changeDetectorRef=d(Le);_elementRef=d(G);_dir=d(Mt,{optional:!0});_idGenerator=d(Ue);_renderer=d(Ye);_parentFormField=d(zd,{optional:!0});ngControl=d(Ji,{self:!0,optional:!0});_liveAnnouncer=d(Cd);_defaultOptions=d(M$,{optional:!0});_animationsDisabled=tt();_popoverLocation;_initialized=new k;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=C0(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=D0(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new L0(this,e)}_scrollStrategyFactory=d(S$);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new k;_errorStateTracker;stateChanges=new k;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=S(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(re.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Ai(()=>{let e=this.options;return e?e.changes.pipe(qe(e),mt(()=>sn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(mt(()=>this.optionSelectionChanges))});openedChange=new Q;_openedStream=this.openedChange.pipe(Ie(e=>e),pe(()=>{}));_closedStream=this.openedChange.pipe(Ie(e=>!e),pe(()=>{}));selectionChange=new Q;valueChange=new Q;constructor(){let e=d(Qo),i=d(Vd,{optional:!0}),r=d(Yt,{optional:!0}),o=d(new On("tabindex"),{optional:!0}),a=d(vd,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new xl(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new $d(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Te(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Te(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(qe(null),Te(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(lt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&Zb(this._trackedModal,"aria-owns",i),mA(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Zb(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!Ot(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!Ot(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ot(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof fl?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Md(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=sn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Te(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),sn(...this.options.map(i=>i._stateChanges)).pipe(Te(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=qt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&rt(o,I$,5)(o,yn,5)(o,sh,5),i&2){let a;q(a=Y())&&(r.customTrigger=a.first),q(a=Y())&&(r.options=a),q(a=Y())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&dt(v$,5)(_$,5)(Pp,5),i&2){let o;q(o=Y())&&(r.trigger=o.first),q(o=Y())&&(r.panel=o.first),q(o=Y())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&F("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(de("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),K("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",W],disableRipple:[2,"disableRipple","disableRipple",W],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:$i(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",W],placeholder:"placeholder",required:[2,"required","required",W],multiple:[2,"multiple","multiple",W],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",W],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",$i],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",W]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Ce([{provide:Ud,useExisting:t},{provide:ah,useExisting:t}]),Pe],ngContentSelectors:b$,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Me(y$),f(0,"div",2,0),F("click",function(){return r.open()}),f(3,"div",3),I(4,C$,2,1,"span",4)(5,x$,3,1,"span",5),p(),f(6,"div",6)(7,"div",7),gr(),f(8,"svg",8),U(9,"path",9),p()()()(),N(10,E$,3,16,"ng-template",10),F("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=$t(1);h(3),de("id",r._valueId),h(),T(r.empty?4:5),h(6),x("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[fl,Pp],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var V0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[Tr,x0,De,dl,Zo,x0]})}return t})();var T$=["switch"],A$=["*"];function R$(t,n){t&1&&(f(0,"span",11),gr(),f(1,"svg",13),U(2,"path",14),p(),f(3,"svg",15),U(4,"path",16),p()())}var k$=new C("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),gh=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},j0=(()=>{class t{_elementRef=d(G);_focusMonitor=d(Fn);_changeDetectorRef=d(Le);defaults=d(k$);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new gh(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=tt();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new Q;toggleChange=new Q;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(_t).load(wi);let e=d(new On("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(Ue).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new gh(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&dt(T$,5),i&2){let o;q(o=Y())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(Et("id",r.id),de("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),Gt(r.color?"mat-"+r.color:""),K("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",W],color:"color",disabled:[2,"disabled","disabled",W],disableRipple:[2,"disableRipple","disableRipple",W],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:$i(e)],checked:[2,"checked","checked",W],hideIcon:[2,"hideIcon","hideIcon",W],disabledInteractive:[2,"disabledInteractive","disabledInteractive",W]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[Ce([{provide:Fd,useExisting:dn(()=>t),multi:!0},{provide:zo,useExisting:t,multi:!0}]),Pe],ngContentSelectors:A$,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(Me(),f(0,"div",1)(1,"button",2,0),F("click",function(){return r._handleClick()}),U(3,"div",3)(4,"span",4),f(5,"span",5)(6,"span",6)(7,"span",7),U(8,"span",8),p(),f(9,"span",9),U(10,"span",10),p(),I(11,R$,5,0,"span",11),p()()(),f(12,"label",12),F("click",function(a){return a.stopPropagation()}),Z(13),p()()),i&2){let o=$t(2);x("labelPosition",r.labelPosition),h(),K("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),x("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),de("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),h(9),x("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),h(),T(r.hideIcon?-1:11),h(),x("for",r.buttonId),de("id",r._labelId)}},dependencies:[$o,ER],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),WR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[j0,De]})}return t})();var N$=[[["caption"]],[["colgroup"],["col"]],"*"],P$=["caption","colgroup, col","*"];function F$(t,n){t&1&&Z(0,2)}function L$(t,n){t&1&&(f(0,"thead",0),it(1,1),p(),f(2,"tbody",0),it(3,2)(4,3),p(),f(5,"tfoot",0),it(6,4),p())}function V$(t,n){t&1&&it(0,1)(1,2)(2,3)(3,4)}var rr=new C("CDK_TABLE");var yh=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkCellDef",""]]})}return t})(),bh=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkHeaderCellDef",""]]})}return t})(),QR=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkFooterCellDef",""]]})}return t})(),Tl=(()=>{class t{_table=d(rr,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(e){this._setNameInput(e)}_name;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(e){e!==this._stickyEnd&&(this._stickyEnd=e,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(e){e&&(this._name=e,this.cssClassFriendlyName=e.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkColumnDef",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,yh,5)(o,bh,5)(o,QR,5),i&2){let a;q(a=Y())&&(r.cell=a.first),q(a=Y())&&(r.headerCell=a.first),q(a=Y())&&(r.footerCell=a.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",W],stickyEnd:[2,"stickyEnd","stickyEnd",W]}})}return t})(),_h=class{constructor(n,e){e.nativeElement.classList.add(...n._columnCssClassName)}},ZR=(()=>{class t extends _h{constructor(){super(d(Tl),d(G))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[ae]})}return t})();var KR=(()=>{class t extends _h{constructor(){let e=d(Tl),i=d(G);super(e,i);let r=e._table?._getCellRole();r&&i.nativeElement.setAttribute("role",r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[ae]})}return t})();var H0=(()=>{class t{template=d(je);_differs=d(Gr);columns;_columnsDiffer;constructor(){}ngOnChanges(e){if(!this._columnsDiffer){let i=e.columns&&e.columns.currentValue||[];this._columnsDiffer=this._differs.find(i).create(),this._columnsDiffer.diff(i)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(e){return this instanceof Xd?e.headerCell.template:this instanceof U0?e.footerCell.template:e.cell.template}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,features:[Pe]})}return t})(),Xd=(()=>{class t extends H0{_table=d(rr,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(je),d(Gr))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",W]},features:[ae,Pe]})}return t})(),U0=(()=>{class t extends H0{_table=d(rr,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(je),d(Gr))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",W]},features:[ae,Pe]})}return t})(),Ch=(()=>{class t extends H0{_table=d(rr,{optional:!0});when;constructor(){super(d(je),d(Gr))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[ae]})}return t})(),Ka=(()=>{class t{_viewContainer=d(ct);cells;context;static mostRecentCellOutlet=null;constructor(){t.mostRecentCellOutlet=this}ngOnDestroy(){t.mostRecentCellOutlet===this&&(t.mostRecentCellOutlet=null)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkCellOutlet",""]]})}return t})(),z0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[Ka],encapsulation:2})}return t})();var $0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[Ka],encapsulation:2})}return t})(),XR=(()=>{class t{templateRef=d(je);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","cdkNoDataRow",""]]})}return t})(),qR=["top","bottom","left","right"],B0=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(n=>this._updateCachedSizes(n)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(n,e,i=!0,r=!0,o,a,s){this._isNativeHtmlTable=n,this._stickCellCss=e,this._isBrowser=i,this._needsPositionStickyOnElement=r,this.direction=o,this._positionListener=a,this._tableInjector=s,this._borderCellCss={top:`${e}-border-elem-top`,bottom:`${e}-border-elem-bottom`,left:`${e}-border-elem-left`,right:`${e}-border-elem-right`}}clearStickyPositioning(n,e){(e.includes("left")||e.includes("right"))&&this._removeFromStickyColumnReplayQueue(n);let i=[];for(let r of n)r.nodeType===r.ELEMENT_NODE&&i.push(r,...Array.from(r.children));ht({write:()=>{for(let r of i)this._removeStickyStyle(r,e)}},{injector:this._tableInjector})}updateStickyColumns(n,e,i,r=!0,o=!0){if(!n.length||!this._isBrowser||!(e.some(R=>R)||i.some(R=>R))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let a=n[0],s=a.children.length,l=this.direction==="rtl",c=l?"right":"left",u=l?"left":"right",m=e.lastIndexOf(!0),v=i.indexOf(!0),_,b,A;o&&this._updateStickyColumnReplayQueue({rows:[...n],stickyStartStates:[...e],stickyEndStates:[...i]}),ht({earlyRead:()=>{_=this._getCellWidths(a,r),b=this._getStickyStartColumnPositions(_,e),A=this._getStickyEndColumnPositions(_,i)},write:()=>{for(let R of n)for(let L=0;L<s;L++){let _e=R.children[L];e[L]&&this._addStickyStyle(_e,c,b[L],L===m),i[L]&&this._addStickyStyle(_e,u,A[L],L===v)}this._positionListener&&_.some(R=>!!R)&&(this._positionListener.stickyColumnsUpdated({sizes:m===-1?[]:_.slice(0,m+1).map((R,L)=>e[L]?R:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:v===-1?[]:_.slice(v).map((R,L)=>i[L+v]?R:null).reverse()}))}},{injector:this._tableInjector})}stickRows(n,e,i){if(!this._isBrowser)return;let r=i==="bottom"?n.slice().reverse():n,o=i==="bottom"?e.slice().reverse():e,a=[],s=[],l=[];ht({earlyRead:()=>{for(let c=0,u=0;c<r.length;c++){if(!o[c])continue;a[c]=u;let m=r[c];l[c]=this._isNativeHtmlTable?Array.from(m.children):[m];let v=this._retrieveElementSize(m).height;u+=v,s[c]=v}},write:()=>{let c=o.lastIndexOf(!0);for(let u=0;u<r.length;u++){if(!o[u])continue;let m=a[u],v=u===c;for(let _ of l[u])this._addStickyStyle(_,i,m,v)}i==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:s,offsets:a,elements:l}):this._positionListener?.stickyFooterRowsUpdated({sizes:s,offsets:a,elements:l})}},{injector:this._tableInjector})}updateStickyFooterContainer(n,e){this._isNativeHtmlTable&&ht({write:()=>{let i=n.querySelector("tfoot");i&&(e.some(r=>!r)?this._removeStickyStyle(i,["bottom"]):this._addStickyStyle(i,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(n,e){if(!n.classList.contains(this._stickCellCss))return;for(let r of e)n.style[r]="",n.classList.remove(this._borderCellCss[r]);qR.some(r=>e.indexOf(r)===-1&&n.style[r])?n.style.zIndex=this._getCalculatedZIndex(n):(n.style.zIndex="",this._needsPositionStickyOnElement&&(n.style.position=""),n.classList.remove(this._stickCellCss))}_addStickyStyle(n,e,i,r){n.classList.add(this._stickCellCss),r&&n.classList.add(this._borderCellCss[e]),n.style[e]=`${i}px`,n.style.zIndex=this._getCalculatedZIndex(n),this._needsPositionStickyOnElement&&(n.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(n){let e={top:100,bottom:10,left:1,right:1},i=0;for(let r of qR)n.style[r]&&(i+=e[r]);return i?`${i}`:""}_getCellWidths(n,e=!0){if(!e&&this._cachedCellWidths.length)return this._cachedCellWidths;let i=[],r=n.children;for(let o=0;o<r.length;o++){let a=r[o];i.push(this._retrieveElementSize(a).width)}return this._cachedCellWidths=i,i}_getStickyStartColumnPositions(n,e){let i=[],r=0;for(let o=0;o<n.length;o++)e[o]&&(i[o]=r,r+=n[o]);return i}_getStickyEndColumnPositions(n,e){let i=[],r=0;for(let o=n.length;o>0;o--)e[o]&&(i[o]=r,r+=n[o]);return i}_retrieveElementSize(n){let e=this._elemSizeCache.get(n);if(e)return e;let i=n.getBoundingClientRect(),r={width:i.width,height:i.height};return this._resizeObserver&&(this._elemSizeCache.set(n,r),this._resizeObserver.observe(n,{box:"border-box"})),r}_updateStickyColumnReplayQueue(n){this._removeFromStickyColumnReplayQueue(n.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(n)}_removeFromStickyColumnReplayQueue(n){let e=new Set(n);for(let i of this._updatedStickyColumnsParamsToReplay)i.rows=i.rows.filter(r=>!e.has(r));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(i=>!!i.rows.length)}_updateCachedSizes(n){let e=!1;for(let i of n){let r=i.borderBoxSize?.length?{width:i.borderBoxSize[0].inlineSize,height:i.borderBoxSize[0].blockSize}:{width:i.contentRect.width,height:i.contentRect.height};r.width!==this._elemSizeCache.get(i.target)?.width&&j$(i.target)&&(e=!0),this._elemSizeCache.set(i.target,r)}e&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let i of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(i.rows,i.stickyStartStates,i.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function j$(t){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(n=>t.classList.contains(n))}var Kd=new C("STICKY_POSITIONING_LISTENER");var G0=(()=>{class t{viewContainer=d(ct);elementRef=d(G);constructor(){let e=d(rr);e._rowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","rowOutlet",""]]})}return t})(),W0=(()=>{class t{viewContainer=d(ct);elementRef=d(G);constructor(){let e=d(rr);e._headerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","headerRowOutlet",""]]})}return t})(),q0=(()=>{class t{viewContainer=d(ct);elementRef=d(G);constructor(){let e=d(rr);e._footerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","footerRowOutlet",""]]})}return t})(),Y0=(()=>{class t{viewContainer=d(ct);elementRef=d(G);constructor(){let e=d(rr);e._noDataRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","noDataRowOutlet",""]]})}return t})(),Q0=(()=>{class t{_differs=d(Gr);_changeDetectorRef=d(Le);_elementRef=d(G);_dir=d(Mt,{optional:!0});_platform=d(ke);_viewRepeater;_viewportRuler=d(Ir);_injector=d(oe);_virtualScrollViewport=d(TT,{optional:!0,host:!0});_positionListener=d(Kd,{optional:!0})||d(Kd,{optional:!0,skipSelf:!0});_document=d(V);_data;_renderedRange;_onDestroy=new k;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new k;_footerRowStickyUpdates=new k;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let e=this._elementRef.nativeElement.getAttribute("role");return e==="grid"||e==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(e){this._trackByFn=e}_trackByFn;get dataSource(){return this._dataSource}set dataSource(e){this._dataSource!==e&&(this._switchDataSource(e),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new k;_dataStream=new k;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(e){this._multiTemplateDataRows=e,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(e){this._fixedLayout=e,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new Q;viewChange=new Ct({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){d(new On("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((i,r)=>this.trackBy?this.trackBy(r.dataIndex,r.data):r)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(Te(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new Mp:new dh,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(e=>{e?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),Sp(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let e=this._dataDiffer.diff(this._renderRows);if(!e){this._updateNoDataRow(),this.contentChanged.next();return}let i=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(e,i,(r,o,a)=>this._getEmbeddedViewArgs(r.item,a),r=>r.item.data,r=>{r.operation===Qi.INSERTED&&r.context&&this._renderCellTemplateForItem(r.record.item.rowDef,r.context)}),this._updateRowIndexContext(),e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);o.context.$implicit=r.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(e){this._customColumnDefs.add(e)}removeColumnDef(e){this._customColumnDefs.delete(e)}addRowDef(e){this._customRowDefs.add(e)}removeRowDef(e){this._customRowDefs.delete(e)}addHeaderRowDef(e){this._customHeaderRowDefs.add(e),this._headerRowDefChanged=!0}removeHeaderRowDef(e){this._customHeaderRowDefs.delete(e),this._headerRowDefChanged=!0}addFooterRowDef(e){this._customFooterRowDefs.add(e),this._footerRowDefChanged=!0}removeFooterRowDef(e){this._customFooterRowDefs.delete(e),this._footerRowDefChanged=!0}setNoDataRow(e){this._customNoDataRow=e}updateStickyHeaderRowStyles(){let e=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let r=YR(this._headerRowOutlet,"thead");r&&(r.style.display=e.length?"":"none")}let i=this._headerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["top"]),this._stickyStyler.stickRows(e,i,"top"),this._headerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyFooterRowStyles(){let e=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let r=YR(this._footerRowOutlet,"tfoot");r&&(r.style.display=e.length?"":"none")}let i=this._footerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["bottom"]),this._stickyStyler.stickRows(e,i,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,i),this._footerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyColumnStyles(){let e=this._getRenderedRows(this._headerRowOutlet),i=this._getRenderedRows(this._rowOutlet),r=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...e,...i,...r],["left","right"]),this._stickyColumnStylesNeedReset=!1),e.forEach((o,a)=>{this._addStickyColumnStyles([o],this._headerRowDefs[a])}),this._rowDefs.forEach(o=>{let a=[];for(let s=0;s<i.length;s++)this._renderRows[s].rowDef===o&&a.push(i[s]);this._addStickyColumnStyles(a,o)}),r.forEach((o,a)=>{this._addStickyColumnStyles([o],this._footerRowDefs[a])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(e){this._positionListener?.stickyColumnsUpdated(e)}stickyEndColumnsUpdated(e){this._positionListener?.stickyEndColumnsUpdated(e)}stickyHeaderRowsUpdated(e){this._headerRowStickyUpdates.next(e),this._positionListener?.stickyHeaderRowsUpdated(e)}stickyFooterRowsUpdated(e){this._footerRowStickyUpdates.next(e),this._positionListener?.stickyFooterRowsUpdated(e)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let i=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||i,this._forceRecalculateCellWidths=i,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let e=[],i=Math.min(this._data.length,this._renderedRange.end),r=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<i;o++){let a=this._data[o],s=this._getRenderRowsForData(a,o,r.get(a));this._cachedRenderRowsMap.has(a)||this._cachedRenderRowsMap.set(a,new WeakMap);for(let l=0;l<s.length;l++){let c=s[l],u=this._cachedRenderRowsMap.get(c.data);u.has(c.rowDef)?u.get(c.rowDef).push(c):u.set(c.rowDef,[c]),e.push(c)}}return e}_getRenderRowsForData(e,i,r){return this._getRowDefs(e,i).map(a=>{let s=r&&r.has(a)?r.get(a):[];if(s.length){let l=s.shift();return l.dataIndex=i,l}else return{data:e,rowDef:a,dataIndex:i}})}_cacheColumnDefs(){this._columnDefsByName.clear(),vh(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(i=>{this._columnDefsByName.has(i.name),this._columnDefsByName.set(i.name,i)})}_cacheRowDefs(){this._headerRowDefs=vh(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=vh(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=vh(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let e=this._rowDefs.filter(i=>!i.when);this._defaultRowDef=e[0]}_renderUpdatedColumns(){let e=(a,s)=>{let l=!!s.getColumnsDiff();return a||l},i=this._rowDefs.reduce(e,!1);i&&this._forceRenderDataRows();let r=this._headerRowDefs.reduce(e,!1);r&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(e,!1);return o&&this._forceRenderFooterRows(),i||r||o}_switchDataSource(e){this._data=[],Sp(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),e||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=e}_observeRenderChanges(){if(!this.dataSource)return;let e;Sp(this.dataSource)?e=this.dataSource.connect(this):la(this.dataSource)?e=this.dataSource:Array.isArray(this.dataSource)&&(e=X(this.dataSource)),this._renderChangeSubscription=sr([e,this.viewChange]).pipe(Te(this._onDestroy)).subscribe(([i,r])=>{this._data=i||[],this._renderedRange=r,this._dataStream.next(i),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((e,i)=>this._renderRow(this._headerRowOutlet,e,i)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((e,i)=>this._renderRow(this._footerRowOutlet,e,i)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(e,i){let r=Array.from(i?.columns||[]).map(s=>{let l=this._columnDefsByName.get(s);return l}),o=r.map(s=>s.sticky),a=r.map(s=>s.stickyEnd);this._stickyStyler.updateStickyColumns(e,o,a,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(e){let i=[];for(let r=0;r<e.viewContainer.length;r++){let o=e.viewContainer.get(r);i.push(o.rootNodes[0])}return i}_getRowDefs(e,i){if(this._rowDefs.length===1)return[this._rowDefs[0]];let r=[];if(this.multiTemplateDataRows)r=this._rowDefs.filter(o=>!o.when||o.when(i,e));else{let o=this._rowDefs.find(a=>a.when&&a.when(i,e))||this._defaultRowDef;o&&r.push(o)}return r.length,r}_getEmbeddedViewArgs(e,i){let r=e.rowDef,o={$implicit:e.data};return{templateRef:r.template,context:o,index:i}}_renderRow(e,i,r,o={}){let a=e.viewContainer.createEmbeddedView(i.template,o,r);return this._renderCellTemplateForItem(i,o),a}_renderCellTemplateForItem(e,i){for(let r of this._getCellTemplates(e))Ka.mostRecentCellOutlet&&Ka.mostRecentCellOutlet._viewContainer.createEmbeddedView(r,i);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let e=this._rowOutlet.viewContainer;for(let i=0,r=e.length;i<r;i++){let a=e.get(i).context;a.count=r,a.first=i===0,a.last=i===r-1,a.even=i%2===0,a.odd=!a.even,this.multiTemplateDataRows?(a.dataIndex=this._renderRows[i].dataIndex,a.renderIndex=i):a.index=this._renderRows[i].dataIndex}}_getCellTemplates(e){return!e||!e.columns?[]:Array.from(e.columns,i=>{let r=this._columnDefsByName.get(i);return e.extractCellTemplate(r)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let e=(i,r)=>i||r.hasStickyChanged();this._headerRowDefs.reduce(e,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(e,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(e,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let e=this._dir?this._dir.value:"ltr",i=this._injector;this._stickyStyler=new B0(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,e,this,i),(this._dir?this._dir.change:X()).pipe(Te(this._onDestroy)).subscribe(r=>{this._stickyStyler.direction=r,this.updateStickyColumnStyles()})}_setupVirtualScrolling(e){let i=typeof requestAnimationFrame<"u"?_u:hu;this.viewChange.next({start:0,end:0}),e.renderedRangeStream.pipe(ls(0,i),Te(this._onDestroy)).subscribe(this.viewChange),e.attach({dataStream:this._dataStream,measureRangeSize:(r,o)=>this._measureRangeSize(r,o)}),sr([e.renderedContentOffset,this._headerRowStickyUpdates]).pipe(Te(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let a=0;a<o.elements.length;a++){let s=o.elements[a];if(s){let l=o.offsets[a],c=r!==0?Math.max(r-l,l):-l;for(let u of s)u.style.top=`${-c}px`}}}),sr([e.renderedContentOffset,this._footerRowStickyUpdates]).pipe(Te(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let a=0;a<o.elements.length;a++){let s=o.elements[a];if(s)for(let l of s)l.style.bottom=`${r+o.offsets[a]}px`}})}_getOwnDefs(e){return e.filter(i=>!i._table||i._table===this)}_updateNoDataRow(){let e=this._customNoDataRow||this._noDataRow;if(!e)return;let i=this._rowOutlet.viewContainer.length===0;if(i===this._isShowingNoDataRow)return;let r=this._noDataRowOutlet.viewContainer;if(i){let o=r.createEmbeddedView(e.templateRef),a=o.rootNodes[0];if(o.rootNodes.length===1&&a?.nodeType===this._document.ELEMENT_NODE){a.setAttribute("role","row"),a.classList.add(...e._contentClassNames);let s=a.querySelectorAll(e._cellSelector);for(let l=0;l<s.length;l++)s[l].classList.add(...e._cellClassNames)}}else r.clear();this._isShowingNoDataRow=i,this._changeDetectorRef.markForCheck()}_measureRangeSize(e,i){if(e.start>=e.end||i!=="vertical")return 0;let r=this.viewChange.value,o=this._rowOutlet.viewContainer;e.start<r.start||e.end>r.end;let a=e.start-r.start,s=e.end-e.start,l,c;for(let v=0;v<s;v++){let _=o.get(v+a);if(_&&_.rootNodes.length){l=c=_.rootNodes[0];break}}for(let v=s-1;v>-1;v--){let _=o.get(v+a);if(_&&_.rootNodes.length){c=_.rootNodes[_.rootNodes.length-1];break}}let u=l?.getBoundingClientRect?.(),m=c?.getBoundingClientRect?.();return u&&m?m.bottom-u.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,XR,5)(o,Tl,5)(o,Ch,5)(o,Xd,5)(o,U0,5),i&2){let a;q(a=Y())&&(r._noDataRow=a.first),q(a=Y())&&(r._contentColumnDefs=a),q(a=Y())&&(r._contentRowDefs=a),q(a=Y())&&(r._contentHeaderRowDefs=a),q(a=Y())&&(r._contentFooterRowDefs=a)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(i,r){i&2&&K("cdk-table-fixed-layout",r.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",W],fixedLayout:[2,"fixedLayout","fixedLayout",W],recycleRows:[2,"recycleRows","recycleRows",W]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[Ce([{provide:rr,useExisting:t},{provide:Kd,useValue:null}])],ngContentSelectors:P$,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(Me(N$),Z(0),Z(1,1),I(2,F$,1,0),I(3,L$,7,0)(4,V$,4,0)),i&2&&(h(2),T(r._isServer?2:-1),h(),T(r._isNativeHtmlTable?3:4))},dependencies:[W0,G0,Y0,q0],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return t})();function vh(t,n){return t.concat(Array.from(n))}function YR(t,n){let e=n.toUpperCase(),i=t.viewContainer.element.nativeElement;for(;i;){let r=i.nodeType===1?i.nodeName:null;if(r===e)return i;if(r==="TABLE")break;i=i.parentNode}return null}var JR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[fd]})}return t})();var B$=["*"];function H$(t,n){t&1&&Z(0)}var Z0=(()=>{class t{_elementRef=d(G);constructor(){}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkStepHeader",""]],hostAttrs:["role","tab"]})}return t})(),K0=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkStepLabel",""]]})}return t})();var Xa={NUMBER:"number",EDIT:"edit",DONE:"done",ERROR:"error"},U$=new C("STEPPER_GLOBAL_OPTIONS"),Dh=(()=>{class t{_stepperOptions;_stepper=d(Al);_displayDefaultIndicatorType;stepLabel;_childForms;content;stepControl;get interacted(){return this._interacted()}set interacted(e){this._interacted.set(e)}_interacted=S(!1);interactedStream=new Q;label;errorMessage;ariaLabel;ariaLabelledby;get state(){return this._state()}set state(e){this._state.set(e)}_state=S(void 0);get editable(){return this._editable()}set editable(e){this._editable.set(e)}_editable=S(!0);optional=!1;get completed(){let e=this._completedOverride(),i=this._interacted();return e??(i&&(!this.stepControl||this.stepControl.valid))}set completed(e){this._completedOverride.set(e)}_completedOverride=S(null);index=S(-1);isSelected=Rt(()=>this._stepper.selectedIndex===this.index());indicatorType=Rt(()=>{let e=this.isSelected(),i=this.completed,r=this._state()??Xa.NUMBER,o=this._editable();return this._showError()&&this.hasError&&!e?Xa.ERROR:this._displayDefaultIndicatorType?!i||e?Xa.NUMBER:o?Xa.EDIT:Xa.DONE:i&&!e?Xa.DONE:i&&e?r:o&&e?Xa.EDIT:r});isNavigable=Rt(()=>{let e=this.isSelected();return this.completed||e||!this._stepper.linear});get hasError(){let e=this._customError();return e??this._getDefaultError()}set hasError(e){this._customError.set(e)}_customError=S(null);_getDefaultError(){return this.interacted&&!!this.stepControl?.invalid}constructor(){let e=d(U$,{optional:!0});this._stepperOptions=e||{},this._displayDefaultIndicatorType=this._stepperOptions.displayDefaultIndicatorType!==!1}select(){this._stepper.selected=this}reset(){this._interacted.set(!1),this._completedOverride()!=null&&this._completedOverride.set(!1),this._customError()!=null&&this._customError.set(!1),this.stepControl&&(this._childForms?.forEach(e=>e.resetForm?.()),this.stepControl.reset())}ngOnChanges(){this._stepper._stateChanged()}_markAsInteracted(){this._interacted()||(this._interacted.set(!0),this.interactedStream.emit(this))}_showError(){return this._stepperOptions.showError??this._customError()!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-step"]],contentQueries:function(i,r,o){if(i&1&&rt(o,K0,5)(o,Xi,5),i&2){let a;q(a=Y())&&(r.stepLabel=a.first),q(a=Y())&&(r._childForms=a)}},viewQuery:function(i,r){if(i&1&&dt(je,7),i&2){let o;q(o=Y())&&(r.content=o.first)}},inputs:{stepControl:"stepControl",label:"label",errorMessage:"errorMessage",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],state:"state",editable:[2,"editable","editable",W],optional:[2,"optional","optional",W],completed:[2,"completed","completed",W],hasError:[2,"hasError","hasError",W]},outputs:{interactedStream:"interacted"},exportAs:["cdkStep"],features:[Pe],ngContentSelectors:B$,decls:1,vars:0,template:function(i,r){i&1&&(Me(),Fs(0,H$,1,0,"ng-template"))},encapsulation:2,changeDetection:0})}return t})(),Al=(()=>{class t{_dir=d(Mt,{optional:!0});_changeDetectorRef=d(Le);_elementRef=d(G);_destroyed=new k;_keyManager;_steps;steps=new Jt;_stepHeader;_sortedHeaders=new Jt;get linear(){return this._linear()}set linear(e){this._linear.set(e)}_linear=S(!1);get selectedIndex(){return this._selectedIndex()}set selectedIndex(e){this._steps?(this._isValidIndex(e),this.selectedIndex!==e&&(this.selected?._markAsInteracted(),!this._anyControlsInvalidOrPending(e)&&(e>=this.selectedIndex||this.steps.toArray()[e].editable)&&this._updateSelectedItemIndex(e))):this._selectedIndex.set(e)}_selectedIndex=S(0);get selected(){return this.steps?this.steps.toArray()[this.selectedIndex]:void 0}set selected(e){this.selectedIndex=e&&this.steps?this.steps.toArray().indexOf(e):-1}selectionChange=new Q;selectedIndexChange=new Q;_groupId=d(Ue).getId("cdk-stepper-");get orientation(){return this._orientation}set orientation(e){this._orientation=e,this._keyManager&&this._keyManager.withVerticalOrientation(e==="vertical")}_orientation="horizontal";constructor(){}ngAfterContentInit(){this._steps.changes.pipe(qe(this._steps),Te(this._destroyed)).subscribe(e=>{this.steps.reset(e.filter(i=>i._stepper===this)),this.steps.forEach((i,r)=>i.index.set(r)),this.steps.notifyOnChanges()})}ngAfterViewInit(){if(this._stepHeader.changes.pipe(qe(this._stepHeader),Te(this._destroyed)).subscribe(e=>{this._sortedHeaders.reset(e.toArray().sort((i,r)=>i._elementRef.nativeElement.compareDocumentPosition(r._elementRef.nativeElement)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1)),this._sortedHeaders.notifyOnChanges()}),this._keyManager=new to(this._sortedHeaders).withWrap().withHomeAndEnd().withVerticalOrientation(this._orientation==="vertical"),this._keyManager.updateActiveItem(this.selectedIndex),(this._dir?this._dir.change:X()).pipe(qe(this._layoutDirection()),Te(this._destroyed)).subscribe(e=>this._keyManager?.withHorizontalOrientation(e)),this._keyManager.updateActiveItem(this.selectedIndex),this.steps.changes.subscribe(()=>{this.selected||this._selectedIndex.set(Math.max(this.selectedIndex-1,0))}),this._isValidIndex(this.selectedIndex)||this._selectedIndex.set(0),this.linear&&this.selectedIndex>0){let e=this.steps.toArray().slice(0,this._selectedIndex());for(let i of e)i._markAsInteracted()}}ngOnDestroy(){this._keyManager?.destroy(),this.steps.destroy(),this._sortedHeaders.destroy(),this._destroyed.next(),this._destroyed.complete()}next(){this.selectedIndex=Math.min(this._selectedIndex()+1,this.steps.length-1)}previous(){this.selectedIndex=Math.max(this._selectedIndex()-1,0)}reset(){this._updateSelectedItemIndex(0),this.steps.forEach(e=>e.reset()),this._stateChanged()}_getStepLabelId(e){return`${this._groupId}-label-${e}`}_getStepContentId(e){return`${this._groupId}-content-${e}`}_stateChanged(){this._changeDetectorRef.markForCheck()}_getAnimationDirection(e){let i=e-this._selectedIndex();return i<0?this._layoutDirection()==="rtl"?"next":"previous":i>0?this._layoutDirection()==="rtl"?"previous":"next":"current"}_getFocusIndex(){return this._keyManager?this._keyManager.activeItemIndex:this._selectedIndex()}_updateSelectedItemIndex(e){let i=this.steps.toArray(),r=this._selectedIndex();this.selectionChange.emit({selectedIndex:e,previouslySelectedIndex:r,selectedStep:i[e],previouslySelectedStep:i[r]}),this._keyManager&&(this._containsFocus()?this._keyManager.setActiveItem(e):this._keyManager.updateActiveItem(e)),this._selectedIndex.set(e),this.selectedIndexChange.emit(e),this._stateChanged()}_onKeydown(e){let i=Ot(e),r=e.keyCode,o=this._keyManager;o?.activeItemIndex!=null&&!i&&(r===32||r===13)?(this.selectedIndex=o.activeItemIndex,e.preventDefault()):o?.setFocusOrigin("keyboard").onKeydown(e)}_anyControlsInvalidOrPending(e){return this.linear&&e>=0?this.steps.toArray().slice(0,e).some(i=>{let r=i.stepControl;return(r?r.invalid||r.pending||!i.interacted:!i.completed)&&!i.optional&&!i._completedOverride()}):!1}_layoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_containsFocus(){let e=this._elementRef.nativeElement,i=Ba();return e===i||e.contains(i)}_isValidIndex(e){return e>-1&&(!this.steps||e<this.steps.length)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkStepper",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,Dh,5)(o,Z0,5),i&2){let a;q(a=Y())&&(r._steps=a),q(a=Y())&&(r._stepHeader=a)}},inputs:{linear:[2,"linear","linear",W],selectedIndex:[2,"selectedIndex","selectedIndex",$i],selected:"selected",orientation:"orientation"},outputs:{selectionChange:"selectionChange",selectedIndexChange:"selectedIndexChange"},exportAs:["cdkStepper"]})}return t})(),ek=(()=>{class t{_stepper=d(Al);type="submit";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["button","cdkStepperNext",""]],hostVars:1,hostBindings:function(i,r){i&1&&F("click",function(){return r._stepper.next()}),i&2&&Et("type",r.type)},inputs:{type:"type"}})}return t})(),tk=(()=>{class t{_stepper=d(Al);type="button";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["button","cdkStepperPrevious",""]],hostVars:1,hostBindings:function(i,r){i&1&&F("click",function(){return r._stepper.previous()}),i&2&&Et("type",r.type)},inputs:{type:"type"}})}return t})(),nk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var z$=(t,n,e)=>({index:t,active:n,optional:e});function $$(t,n){if(t&1&&it(0,2),t&2){let e=y();x("ngTemplateOutlet",e.iconOverrides[e.state])("ngTemplateOutletContext",uy(2,z$,e.index,e.active,e.optional))}}function G$(t,n){if(t&1&&(f(0,"span",7),g(1),p()),t&2){let e=y(2);h(),ve(e._getDefaultTextForState(e.state))}}function W$(t,n){if(t&1&&(f(0,"span",8),g(1),p()),t&2){let e=y(3);h(),ve(e._intl.completedLabel)}}function q$(t,n){if(t&1&&(f(0,"span",8),g(1),p()),t&2){let e=y(3);h(),ve(e._intl.editableLabel)}}function Y$(t,n){if(t&1&&(I(0,W$,2,1,"span",8)(1,q$,2,1,"span",8),f(2,"mat-icon",7),g(3),p()),t&2){let e=y(2);T(e.state==="done"?0:e.state==="edit"?1:-1),h(3),ve(e._getDefaultTextForState(e.state))}}function Q$(t,n){if(t&1&&I(0,G$,2,1,"span",7)(1,Y$,4,2),t&2){let e,i=y();T((e=i.state)==="number"?0:1)}}function Z$(t,n){t&1&&(f(0,"div",4),it(1,9),p()),t&2&&(h(),x("ngTemplateOutlet",n.template))}function K$(t,n){if(t&1&&(f(0,"div",4),g(1),p()),t&2){let e=y();h(),ve(e.label)}}function X$(t,n){if(t&1&&(f(0,"div",5),g(1),p()),t&2){let e=y();h(),ve(e._intl.optionalLabel)}}function J$(t,n){if(t&1&&(f(0,"div",6),g(1),p()),t&2){let e=y();h(),ve(e.errorMessage)}}var ik=["*"];function e3(t,n){}function t3(t,n){if(t&1&&(Z(0),N(1,e3,0,0,"ng-template",0)),t&2){let e=y();h(),x("cdkPortalOutlet",e._portal)}}var n3=["animatedContainer"],rk=t=>({steps:t}),ok=t=>({step:t});function i3(t,n){t&1&&Z(0)}function r3(t,n){if(t&1&&(f(0,"div",5),it(1,9)(2,6),p()),t&2){let e=y(2),i=$t(6);h(),x("ngTemplateOutlet",e.headerPrefix()),h(),x("ngTemplateOutlet",i)("ngTemplateOutletContext",Vs(3,rk,e.steps))}}function o3(t,n){if(t&1&&it(0,6),t&2){let e=y(2),i=$t(6);x("ngTemplateOutlet",i)("ngTemplateOutletContext",Vs(2,rk,e.steps))}}function a3(t,n){if(t&1&&(f(0,"div",10,2),it(2,9),p()),t&2){let e=n.$implicit,i=n.$index,r=y(2);Gt("mat-horizontal-stepper-content-"+r._getAnimationDirection(i)),x("id",r._getStepContentId(i)),de("aria-labelledby",r._getStepLabelId(i))("inert",r.selectedIndex===i?null:""),h(2),x("ngTemplateOutlet",e.content)}}function s3(t,n){if(t&1&&(f(0,"div",3),I(1,r3,3,5,"div",5)(2,o3,1,4,"ng-container",6),f(3,"div",7),Ze(4,a3,3,6,"div",8,Qe),p()()),t&2){let e=y();h(),T(e.headerPrefix()?1:2),h(3),Ke(e.steps)}}function l3(t,n){if(t&1&&it(0,9),t&2){let e=y(2);x("ngTemplateOutlet",e.headerPrefix())}}function c3(t,n){if(t&1&&(f(0,"div",11),it(1,6),f(2,"div",12,2)(4,"div",13)(5,"div",14),it(6,9),p()()()()),t&2){let e=n.$implicit,i=n.$index,r=n.$index,o=n.$count,a=y(2),s=$t(4);h(),x("ngTemplateOutlet",s)("ngTemplateOutletContext",Vs(11,ok,e)),h(),K("mat-stepper-vertical-line",r!==o-1)("mat-vertical-content-container-active",a.selectedIndex===i),de("inert",a.selectedIndex===i?null:"")("aria-label",a.ariaLabel),h(2),x("id",a._getStepContentId(i)),de("aria-labelledby",a._getStepLabelId(i)),h(2),x("ngTemplateOutlet",e.content)}}function d3(t,n){if(t&1&&(f(0,"div",4),I(1,l3,1,1,"ng-container",9),Ze(2,c3,7,13,"div",11,Qe),p()),t&2){let e=y();h(),T(e.headerPrefix()?1:-1),h(),Ke(e.steps)}}function u3(t,n){if(t&1){let e=ze();f(0,"mat-step-header",15),F("click",function(){let r=Ae(e).step;return Re(r.select())})("keydown",function(r){Ae(e);let o=y();return Re(o._onKeydown(r))}),p()}if(t&2){let e=n.step,i=y();K("mat-horizontal-stepper-header",i.orientation==="horizontal")("mat-vertical-stepper-header",i.orientation==="vertical"),x("tabIndex",i._getFocusIndex()===e.index()?0:-1)("id",i._getStepLabelId(e.index()))("index",e.index())("state",e.indicatorType())("label",e.stepLabel||e.label)("selected",e.isSelected())("active",e.isNavigable())("optional",e.optional)("errorMessage",e.errorMessage)("iconOverrides",i._iconOverrides)("disableRipple",i.disableRipple||!e.isNavigable())("color",e.color||i.color),de("role",i.orientation==="horizontal"?"tab":"button")("aria-posinset",i.orientation==="horizontal"?e.index()+1:null)("aria-setsize",i.orientation==="horizontal"?i.steps.length:null)("aria-selected",i.orientation==="horizontal"?e.isSelected():null)("aria-current",i.orientation==="vertical"&&e.isSelected()?"step":null)("aria-disabled",i.orientation==="vertical"&&e.isSelected()?"true":null)("aria-expanded",i.orientation==="vertical"?e.isSelected():null)("aria-controls",i._getStepContentId(e.index()))("aria-label",e.ariaLabel||null)("aria-labelledby",!e.ariaLabel&&e.ariaLabelledby?e.ariaLabelledby:null)("aria-disabled",e.isNavigable()?null:!0)}}function f3(t,n){t&1&&U(0,"div",17)}function m3(t,n){if(t&1&&(it(0,6),I(1,f3,1,0,"div",17)),t&2){let e=n.$implicit,i=n.$index,r=n.$count;y(2);let o=$t(4);x("ngTemplateOutlet",o)("ngTemplateOutletContext",Vs(3,ok,e)),h(),T(i!==r-1?1:-1)}}function p3(t,n){if(t&1&&(f(0,"div",16),Ze(1,m3,2,5,null,null,Qe),p()),t&2){let e=n.steps,i=y();de("aria-label",i.ariaLabel),h(),Ke(e)}}var Jd=(()=>{class t extends K0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matStepLabel",""]],features:[ae]})}return t})(),h3=(()=>{class t{changes=new k;optionalLabel="Optional";completedLabel="Completed";editableLabel="Editable";static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),X0=(()=>{class t extends Z0{_intl=d(h3);_focusMonitor=d(Fn);_intlSubscription;state;label;errorMessage;iconOverrides;index;selected=!1;active=!1;optional=!1;disableRipple=!1;color;constructor(){super();let e=d(_t);e.load(wi),e.load(hl);let i=d(Le);this._intlSubscription=this._intl.changes.subscribe(()=>i.markForCheck())}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){this._intlSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._elementRef)}focus(e,i){e?this._focusMonitor.focusVia(this._elementRef,e,i):this._elementRef.nativeElement.focus(i)}_stringLabel(){return this.label instanceof Jd?null:this.label}_templateLabel(){return this.label instanceof Jd?this.label:null}_getHostElement(){return this._elementRef.nativeElement}_getDefaultTextForState(e){return e=="number"?`${this.index+1}`:e=="edit"?"create":e=="error"?"warning":e}_hasEmptyLabel(){return!this._stringLabel()&&!this._templateLabel()&&!this._hasOptionalLabel()&&!this._hasErrorLabel()}_hasOptionalLabel(){return this.optional&&this.state!=="error"}_hasErrorLabel(){return this.state==="error"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-step-header"]],hostAttrs:["role","",1,"mat-step-header"],hostVars:4,hostBindings:function(i,r){i&2&&(Gt("mat-"+(r.color||"primary")),K("mat-step-header-empty-label",r._hasEmptyLabel()))},inputs:{state:"state",label:"label",errorMessage:"errorMessage",iconOverrides:"iconOverrides",index:"index",selected:"selected",active:"active",optional:"optional",disableRipple:"disableRipple",color:"color"},features:[ae],decls:10,vars:17,consts:[["matRipple","",1,"mat-step-header-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"],[1,"mat-step-icon-content"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-step-label"],[1,"mat-step-text-label"],[1,"mat-step-optional"],[1,"mat-step-sub-label-error"],["aria-hidden","true"],[1,"cdk-visually-hidden"],[3,"ngTemplateOutlet"]],template:function(i,r){if(i&1&&(U(0,"div",0),f(1,"div")(2,"div",1),I(3,$$,1,6,"ng-container",2)(4,Q$,2,1),p()(),f(5,"div",3),I(6,Z$,2,1,"div",4)(7,K$,2,1,"div",4),I(8,X$,2,1,"div",5),I(9,J$,2,1,"div",6),p()),i&2){let o;x("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disableRipple),h(),Gt(dy("mat-step-icon-state-",r.state," mat-step-icon")),K("mat-step-icon-selected",r.selected),h(2),T(r.iconOverrides&&r.iconOverrides[r.state]?3:4),h(2),K("mat-step-label-active",r.active)("mat-step-label-selected",r.selected)("mat-step-label-error",r.state=="error"),h(),T((o=r._templateLabel())?6:r._stringLabel()?7:-1,o),h(2),T(r._hasOptionalLabel()?8:-1),h(),T(r._hasErrorLabel()?9:-1)}},dependencies:[$o,zs,Bn],styles:[`.mat-step-header {
  overflow: hidden;
  outline: none;
  cursor: pointer;
  position: relative;
  box-sizing: content-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-step-header:focus-visible .mat-focus-indicator::before {
  content: "";
}
.mat-step-header:hover[aria-disabled=true] {
  cursor: default;
}
.mat-step-header:hover:not([aria-disabled]), .mat-step-header:hover[aria-disabled=false] {
  background-color: var(--mat-stepper-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-hover-state-layer-shape, var(--mat-sys-corner-medium));
}
.mat-step-header.cdk-keyboard-focused, .mat-step-header.cdk-program-focused {
  background-color: var(--mat-stepper-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-focus-state-layer-shape, var(--mat-sys-corner-medium));
}
@media (hover: none) {
  .mat-step-header:hover {
    background: none;
  }
}
@media (forced-colors: active) {
  .mat-step-header {
    outline: solid 1px;
  }
  .mat-step-header[aria-selected=true] .mat-step-label {
    text-decoration: underline;
  }
  .mat-step-header[aria-disabled=true] {
    outline-color: GrayText;
  }
  .mat-step-header[aria-disabled=true] .mat-step-label,
  .mat-step-header[aria-disabled=true] .mat-step-icon,
  .mat-step-header[aria-disabled=true] .mat-step-optional {
    color: GrayText;
  }
}

.mat-step-optional {
  font-size: 12px;
  color: var(--mat-stepper-header-optional-label-text-color, var(--mat-sys-on-surface-variant));
}

.mat-step-sub-label-error {
  font-size: 12px;
  font-weight: normal;
}

.mat-step-icon {
  border-radius: 50%;
  height: 24px;
  width: 24px;
  flex-shrink: 0;
  position: relative;
  color: var(--mat-stepper-header-icon-foreground-color, var(--mat-sys-surface));
  background-color: var(--mat-stepper-header-icon-background-color, var(--mat-sys-on-surface-variant));
}

.mat-step-icon-content {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
}

.mat-step-icon .mat-icon {
  font-size: 16px;
  height: 16px;
  width: 16px;
}

.mat-step-icon-state-error {
  background-color: var(--mat-stepper-header-error-state-icon-background-color, transparent);
  color: var(--mat-stepper-header-error-state-icon-foreground-color, var(--mat-sys-error));
}
.mat-step-icon-state-error .mat-icon {
  font-size: 24px;
  height: 24px;
  width: 24px;
}

.mat-step-label {
  display: inline-block;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  min-width: 50px;
  vertical-align: middle;
  font-family: var(--mat-stepper-header-label-text-font, var(--mat-sys-title-small-font));
  font-size: var(--mat-stepper-header-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-label-text-weight, var(--mat-sys-title-small-weight));
  color: var(--mat-stepper-header-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-active {
  color: var(--mat-stepper-header-selected-state-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-error {
  color: var(--mat-stepper-header-error-state-label-text-color, var(--mat-sys-error));
  font-size: var(--mat-stepper-header-error-state-label-text-size, var(--mat-sys-title-small-size));
}
.mat-step-label.mat-step-label-selected {
  font-size: var(--mat-stepper-header-selected-state-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-selected-state-label-text-weight, var(--mat-sys-title-small-weight));
}
.mat-step-header-empty-label .mat-step-label {
  min-width: 0;
}

.mat-step-text-label {
  text-overflow: ellipsis;
  overflow: hidden;
}

.mat-step-header .mat-step-header-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-step-icon-selected {
  background-color: var(--mat-stepper-header-selected-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-selected-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-done {
  background-color: var(--mat-stepper-header-done-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-done-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-edit {
  background-color: var(--mat-stepper-header-edit-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-edit-state-icon-foreground-color, var(--mat-sys-on-primary));
}
`],encapsulation:2,changeDetection:0})}return t})(),g3=(()=>{class t{templateRef=d(je);name;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","matStepperIcon",""]],inputs:{name:[0,"matStepperIcon","name"]}})}return t})(),v3=(()=>{class t{_template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","matStepContent",""]]})}return t})(),J0=(()=>{class t extends Dh{_errorStateMatcher=d(Qo,{skipSelf:!0});_viewContainerRef=d(ct);_isSelected=me.EMPTY;stepLabel=void 0;color;_lazyContent;_portal;ngAfterContentInit(){this._isSelected=this._stepper.steps.changes.pipe(mt(()=>this._stepper.selectionChange.pipe(pe(e=>e.selectedStep===this),qe(this._stepper.selected===this)))).subscribe(e=>{e&&this._lazyContent&&!this._portal&&(this._portal=new _n(this._lazyContent._template,this._viewContainerRef))})}ngOnDestroy(){this._isSelected.unsubscribe()}isErrorState(e,i){let r=this._errorStateMatcher.isErrorState(e,i),o=!!(e&&e.invalid&&this.interacted);return r||o}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-step"]],contentQueries:function(i,r,o){if(i&1&&rt(o,Jd,5)(o,v3,5),i&2){let a;q(a=Y())&&(r.stepLabel=a.first),q(a=Y())&&(r._lazyContent=a.first)}},hostAttrs:["hidden",""],inputs:{color:"color"},exportAs:["matStep"],features:[Ce([{provide:Qo,useExisting:t},{provide:Dh,useExisting:t}]),ae],ngContentSelectors:ik,decls:1,vars:0,consts:[[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Me(),N(0,t3,2,1,"ng-template"))},dependencies:[vi],encapsulation:2,changeDetection:0})}return t})(),eC=(()=>{class t extends Al{_ngZone=d(ie);_renderer=d(Ye);_animationsDisabled=tt();_cleanupTransition;_isAnimating=S(!1);_stepHeader=void 0;_animatedContainers;_steps=void 0;steps=new Jt;_icons;animationDone=new Q;disableRipple=!1;color;labelPosition="end";headerPosition="top";ariaLabel=null;headerPrefix=km(null);_iconOverrides={};get animationDuration(){return this._animationDuration}set animationDuration(e){this._animationDuration=/^\d+$/.test(e)?e+"ms":e}_animationDuration="";_isServer=!d(ke).isBrowser;constructor(){super();let i=d(G).nativeElement.nodeName.toLowerCase();this.orientation=i==="mat-vertical-stepper"?"vertical":"horizontal"}ngAfterContentInit(){super.ngAfterContentInit(),this._icons.forEach(({name:e,templateRef:i})=>this._iconOverrides[e]=i),this.steps.changes.pipe(Te(this._destroyed)).subscribe(()=>this._stateChanged()),this.selectedIndexChange.pipe(Te(this._destroyed)).subscribe(()=>{let e=this._getAnimationDuration();e==="0ms"||e==="0s"?this._onAnimationDone():this._isAnimating.set(!0)}),this._ngZone.runOutsideAngular(()=>{this._animationsDisabled||setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-stepper-animations-enabled"),this._cleanupTransition=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionend)},200)})}ngAfterViewInit(){if(super.ngAfterViewInit(),typeof queueMicrotask=="function"){let e=!1;this._animatedContainers.changes.pipe(qe(null),Te(this._destroyed)).subscribe(()=>queueMicrotask(()=>{e||(e=!0,this.animationDone.emit()),this._stateChanged()}))}}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransition?.()}_getAnimationDuration(){return this._animationsDisabled?"0ms":this.animationDuration?this.animationDuration:this.orientation==="horizontal"?"500ms":"225ms"}_handleTransitionend=e=>{let i=e.target;if(!i)return;let r=this.orientation==="horizontal"&&e.propertyName==="transform"&&i.classList.contains("mat-horizontal-stepper-content-current"),o=this.orientation==="vertical"&&e.propertyName==="grid-template-rows"&&i.classList.contains("mat-vertical-content-container-active");(r||o)&&this._animatedContainers.find(s=>s.nativeElement===i)&&this._onAnimationDone()};_onAnimationDone(){this._isAnimating.set(!1),this.animationDone.emit()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-stepper"],["mat-vertical-stepper"],["mat-horizontal-stepper"],["","matStepper",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,J0,5)(o,g3,5),i&2){let a;q(a=Y())&&(r._steps=a),q(a=Y())&&(r._icons=a)}},viewQuery:function(i,r){if(i&1&&dt(X0,5)(n3,5),i&2){let o;q(o=Y())&&(r._stepHeader=o),q(o=Y())&&(r._animatedContainers=o)}},hostVars:14,hostBindings:function(i,r){i&2&&(E("--mat-stepper-animation-duration",r._getAnimationDuration()),K("mat-stepper-horizontal",r.orientation==="horizontal")("mat-stepper-vertical",r.orientation==="vertical")("mat-stepper-label-position-end",r.orientation==="horizontal"&&r.labelPosition=="end")("mat-stepper-label-position-bottom",r.orientation==="horizontal"&&r.labelPosition=="bottom")("mat-stepper-header-position-bottom",r.headerPosition==="bottom")("mat-stepper-animating",r._isAnimating()))},inputs:{disableRipple:"disableRipple",color:"color",labelPosition:"labelPosition",headerPosition:"headerPosition",ariaLabel:[0,"aria-label","ariaLabel"],headerPrefix:[1,"headerPrefix"],animationDuration:"animationDuration"},outputs:{animationDone:"animationDone"},exportAs:["matStepper","matVerticalStepper","matHorizontalStepper"],features:[Ce([{provide:Al,useExisting:t}]),ae],ngContentSelectors:ik,decls:7,vars:2,consts:[["stepTemplate",""],["horizontalStepsTemplate",""],["animatedContainer",""],[1,"mat-horizontal-stepper-wrapper"],[1,"mat-vertical-stepper-wrapper"],[1,"mat-horizontal-stepper-header-wrapper"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-horizontal-content-container"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id","class"],[3,"ngTemplateOutlet"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id"],[1,"mat-step"],[1,"mat-vertical-content-container"],["role","region",1,"mat-vertical-stepper-content",3,"id"],[1,"mat-vertical-content"],[3,"click","keydown","tabIndex","id","index","state","label","selected","active","optional","errorMessage","iconOverrides","disableRipple","color"],["aria-orientation","horizontal","role","tablist",1,"mat-horizontal-stepper-header-container"],[1,"mat-stepper-horizontal-line"]],template:function(i,r){if(i&1&&(Me(),I(0,i3,1,0),I(1,s3,6,1,"div",3)(2,d3,4,1,"div",4),N(3,u3,1,27,"ng-template",null,0,Bs)(5,p3,3,1,"ng-template",null,1,Bs)),i&2){let o;T(r._isServer?0:-1),h(),T((o=r.orientation)==="horizontal"?1:o==="vertical"?2:-1)}},dependencies:[zs,X0],styles:[`.mat-stepper-vertical,
.mat-stepper-horizontal {
  display: block;
  font-family: var(--mat-stepper-container-text-font, var(--mat-sys-body-medium-font));
  background: var(--mat-stepper-container-color, var(--mat-sys-surface));
}

.mat-horizontal-stepper-header-wrapper {
  align-items: center;
  display: flex;
}

.mat-horizontal-stepper-header-container {
  white-space: nowrap;
  display: flex;
  align-items: center;
  flex-grow: 1;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header-container {
  align-items: flex-start;
}
.mat-stepper-header-position-bottom .mat-horizontal-stepper-header-container {
  order: 1;
}

.mat-stepper-horizontal-line {
  border-top-width: 1px;
  border-top-style: solid;
  flex: auto;
  height: 0;
  margin: 0 -16px;
  min-width: 32px;
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-stepper-horizontal-line {
  margin: 0;
  min-width: 0;
  position: relative;
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}

.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  border-top-width: 1px;
  border-top-style: solid;
  content: "";
  display: inline-block;
  height: 0;
  position: absolute;
  width: calc(50% - 20px);
}

.mat-horizontal-stepper-header {
  display: flex;
  overflow: hidden;
  align-items: center;
  padding: 0 24px;
  height: var(--mat-stepper-header-height, 72px);
}
.mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 8px;
  flex: none;
}
[dir=rtl] .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 8px;
}
.mat-horizontal-stepper-header.mat-step-header-empty-label .mat-step-icon {
  margin: 0;
}
.mat-horizontal-stepper-header::before, .mat-horizontal-stepper-header::after {
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header::after {
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  box-sizing: border-box;
  flex-direction: column;
  height: auto;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  right: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before {
  left: 0;
}
[dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:last-child::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:first-child::after {
  display: none;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-label {
  padding: 16px 0 0 0;
  text-align: center;
  width: 100%;
}

.mat-vertical-stepper-header {
  display: flex;
  align-items: center;
  height: 24px;
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-vertical-stepper-header .mat-step-icon {
  margin-right: 12px;
}
[dir=rtl] .mat-vertical-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 12px;
}

.mat-horizontal-stepper-wrapper {
  display: flex;
  flex-direction: column;
}

.mat-horizontal-stepper-content {
  visibility: hidden;
  overflow: hidden;
  outline: 0;
  height: 0;
}
.mat-stepper-animations-enabled .mat-horizontal-stepper-content {
  transition: transform var(--mat-stepper-animation-duration, 0) cubic-bezier(0.35, 0, 0.25, 1);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-previous {
  transform: translate3d(-100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-next {
  transform: translate3d(100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  visibility: visible;
  transform: none;
  height: auto;
}
.mat-stepper-horizontal:not(.mat-stepper-animating) .mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  overflow: visible;
}

.mat-horizontal-content-container {
  overflow: hidden;
  padding: 0 24px 24px 24px;
}
@media (forced-colors: active) {
  .mat-horizontal-content-container {
    outline: solid 1px;
  }
}
.mat-stepper-header-position-bottom .mat-horizontal-content-container {
  padding: 24px 24px 0 24px;
}

.mat-vertical-content-container {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
  margin-left: 36px;
  border: 0;
  position: relative;
}
.mat-stepper-animations-enabled .mat-vertical-content-container {
  transition: grid-template-rows var(--mat-stepper-animation-duration, 0) cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-vertical-content-container.mat-vertical-content-container-active {
  grid-template-rows: 1fr;
}
.mat-step:last-child .mat-vertical-content-container {
  border: none;
}
@media (forced-colors: active) {
  .mat-vertical-content-container {
    outline: solid 1px;
  }
}
[dir=rtl] .mat-vertical-content-container {
  margin-left: 0;
  margin-right: 36px;
}
@supports not (grid-template-rows: 0fr) {
  .mat-vertical-content-container {
    height: 0;
  }
  .mat-vertical-content-container.mat-vertical-content-container-active {
    height: auto;
  }
}

.mat-stepper-vertical-line::before {
  content: "";
  position: absolute;
  left: 0;
  border-left-width: 1px;
  border-left-style: solid;
  border-left-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
  top: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
  bottom: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
}
[dir=rtl] .mat-stepper-vertical-line::before {
  left: auto;
  right: 0;
}

.mat-vertical-stepper-content {
  overflow: hidden;
  outline: 0;
  visibility: hidden;
}
.mat-stepper-animations-enabled .mat-vertical-stepper-content {
  transition: visibility var(--mat-stepper-animation-duration, 0) linear;
}
.mat-vertical-content-container-active > .mat-vertical-stepper-content {
  visibility: visible;
}

.mat-vertical-content {
  padding: 0 24px 24px 24px;
}
`],encapsulation:2,changeDetection:0})}return t})(),ak=(()=>{class t extends ek{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["button","matStepperNext",""]],hostAttrs:[1,"mat-stepper-next"],hostVars:1,hostBindings:function(i,r){i&2&&Et("type",r.type)},features:[ae]})}return t})(),sk=(()=>{class t extends tk{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["button","matStepperPrevious",""]],hostAttrs:[1,"mat-stepper-previous"],hostVars:1,hostBindings:function(i,r){i&2&&Et("type",r.type)},features:[ae]})}return t})(),lk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({providers:[Qo],imports:[_i,nk,Ko,Go,eC,X0,De]})}return t})();var y3=[[["caption"]],[["colgroup"],["col"]],"*"],b3=["caption","colgroup, col","*"];function C3(t,n){t&1&&Z(0,2)}function D3(t,n){t&1&&(f(0,"thead",0),it(1,1),p(),f(2,"tbody",2),it(3,3)(4,4),p(),f(5,"tfoot",0),it(6,5),p())}function w3(t,n){t&1&&it(0,1)(1,3)(2,4)(3,5)}var Cn=(()=>{class t extends Q0{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(i,r){i&2&&K("mat-table-fixed-layout",r.fixedLayout)},exportAs:["matTable"],features:[Ce([{provide:Q0,useExisting:t},{provide:rr,useExisting:t},{provide:Kd,useValue:null}]),ae],ngContentSelectors:b3,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(Me(y3),Z(0),Z(1,1),I(2,C3,1,0),I(3,D3,7,0)(4,w3,4,0)),i&2&&(h(2),T(r._isServer?2:-1),h(),T(r._isNativeHtmlTable?3:4))},dependencies:[W0,G0,Y0,q0],styles:[`.mat-mdc-table-sticky {
  position: sticky !important;
}

mat-table {
  display: block;
}

mat-header-row {
  min-height: var(--mat-table-header-container-height, 56px);
}

mat-row {
  min-height: var(--mat-table-row-item-container-height, 52px);
}

mat-footer-row {
  min-height: var(--mat-table-footer-container-height, 52px);
}

mat-row, mat-header-row, mat-footer-row {
  display: flex;
  border-width: 0;
  border-bottom-width: 1px;
  border-style: solid;
  align-items: center;
  box-sizing: border-box;
}

mat-cell:first-of-type, mat-header-cell:first-of-type, mat-footer-cell:first-of-type {
  padding-left: 24px;
}
[dir=rtl] mat-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:first-of-type:not(:only-of-type) {
  padding-left: 0;
  padding-right: 24px;
}
mat-cell:last-of-type, mat-header-cell:last-of-type, mat-footer-cell:last-of-type {
  padding-right: 24px;
}
[dir=rtl] mat-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:last-of-type:not(:only-of-type) {
  padding-right: 0;
  padding-left: 24px;
}

mat-cell, mat-header-cell, mat-footer-cell {
  flex: 1;
  display: flex;
  align-items: center;
  overflow: hidden;
  word-wrap: break-word;
  min-height: inherit;
}

.mat-mdc-table {
  min-width: 100%;
  border: 0;
  border-spacing: 0;
  table-layout: auto;
  white-space: normal;
  background-color: var(--mat-table-background-color, var(--mat-sys-surface));
}

.mat-table-fixed-layout {
  table-layout: fixed;
}

.mdc-data-table__cell {
  box-sizing: border-box;
  overflow: hidden;
  text-align: start;
  text-overflow: ellipsis;
}

.mdc-data-table__cell,
.mdc-data-table__header-cell {
  padding: 0 16px;
}

.mat-mdc-header-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-header-container-height, 56px);
  color: var(--mat-table-header-headline-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-header-headline-font, var(--mat-sys-title-small-font, Roboto, sans-serif));
  line-height: var(--mat-table-header-headline-line-height, var(--mat-sys-title-small-line-height));
  font-size: var(--mat-table-header-headline-size, var(--mat-sys-title-small-size, 14px));
  font-weight: var(--mat-table-header-headline-weight, var(--mat-sys-title-small-weight, 500));
}

.mat-mdc-row {
  height: var(--mat-table-row-item-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
}

.mat-mdc-row,
.mdc-data-table__content {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-table-row-item-label-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-row-item-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-row-item-label-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-row-item-label-text-weight, var(--mat-sys-body-medium-weight));
}

.mat-mdc-footer-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-footer-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-footer-supporting-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-footer-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-footer-supporting-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-footer-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-table-footer-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}

.mat-mdc-header-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-header-headline-tracking, var(--mat-sys-title-small-tracking));
  font-weight: inherit;
  line-height: inherit;
  box-sizing: border-box;
  text-overflow: ellipsis;
  overflow: hidden;
  outline: none;
  text-align: start;
}
.mdc-data-table__row:last-child > .mat-mdc-header-cell {
  border-bottom: none;
}

.mat-mdc-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
  line-height: inherit;
}
.mdc-data-table__row:last-child > .mat-mdc-cell {
  border-bottom: none;
}

.mat-mdc-footer-cell {
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
}

mat-row.mat-mdc-row,
mat-header-row.mat-mdc-header-row,
mat-footer-row.mat-mdc-footer-row {
  border-bottom: none;
}

.mat-mdc-table tbody,
.mat-mdc-table tfoot,
.mat-mdc-table thead,
.mat-mdc-cell,
.mat-mdc-footer-cell,
.mat-mdc-header-row,
.mat-mdc-row,
.mat-mdc-footer-row,
.mat-mdc-table .mat-mdc-header-cell {
  background: inherit;
}

.mat-mdc-table mat-header-row.mat-mdc-header-row,
.mat-mdc-table mat-row.mat-mdc-row,
.mat-mdc-table mat-footer-row.mat-mdc-footer-cell {
  height: unset;
}

mat-header-cell.mat-mdc-header-cell,
mat-cell.mat-mdc-cell,
mat-footer-cell.mat-mdc-footer-cell {
  align-self: stretch;
}
`],encapsulation:2})}return t})(),Dn=(()=>{class t extends yh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matCellDef",""]],features:[Ce([{provide:yh,useExisting:t}]),ae]})}return t})(),wn=(()=>{class t extends bh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matHeaderCellDef",""]],features:[Ce([{provide:bh,useExisting:t}]),ae]})}return t})();var xn=(()=>{class t extends Tl{get name(){return this._name}set name(e){this._setNameInput(e)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[Ce([{provide:Tl,useExisting:t}]),ae]})}return t})(),En=(()=>{class t extends ZR{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[ae]})}return t})();var Sn=(()=>{class t extends KR{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[ae]})}return t})();var Mn=(()=>{class t extends Xd{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",W]},features:[Ce([{provide:Xd,useExisting:t}]),ae]})}return t})();var In=(()=>{class t extends Ch{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[Ce([{provide:Ch,useExisting:t}]),ae]})}return t})(),Tn=(()=>{class t extends z0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[Ce([{provide:z0,useExisting:t}]),ae],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[Ka],encapsulation:2})}return t})();var An=(()=>{class t extends $0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Fe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[Ce([{provide:$0,useExisting:t}]),ae],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[Ka],encapsulation:2})}return t})();var tC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[JR,De]})}return t})();var x3=["*",[["mat-toolbar-row"]]],E3=["*","mat-toolbar-row"],S3=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),ck=(()=>{class t{_elementRef=d(G);_platform=d(ke);_document=d(V);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&rt(o,S3,5),i&2){let a;q(a=Y())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(Gt(r.color?"mat-"+r.color:""),K("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:E3,decls:2,vars:0,template:function(i,r){i&1&&(Me(x3),Z(0),Z(1,1))},styles:[`.mat-toolbar {
  background: var(--mat-toolbar-container-background-color, var(--mat-sys-surface));
  color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--mat-toolbar-title-text-font, var(--mat-sys-title-large-font));
  font-size: var(--mat-toolbar-title-text-size, var(--mat-sys-title-large-size));
  line-height: var(--mat-toolbar-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-weight: var(--mat-toolbar-title-text-weight, var(--mat-sys-title-large-weight));
  letter-spacing: var(--mat-toolbar-title-text-tracking, var(--mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --mat-button-text-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
  --mat-button-outlined-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var dk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[De]})}return t})();var Si=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[Wo,Ko,Jb,E0,Zo,O0,V0,N0,u0,Ra]})},Mi=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[uh,Wo,Ko,tC,Ra]})},wh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[uh,Wo,Ko,E0,Zo,O0,V0,N0,u0,Ra]})},xh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[dk,Ko,GR,WR,ZA]})},Xo=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=H({type:t});static \u0275inj=B({imports:[uh,Wo,Ko,tC,Ra,Jb]})};var ge=class t{static snackBarConfig(){let n=new qo;return n.duration=12e3,n.panelClass=["btn","btn-outline-dark"],n.verticalPosition="top",n.horizontalPosition="center",n}static openSnackBar(n,e,i){let r=t.snackBarConfig();i.open(n,e,r)}static getMatDialogConf(){let n=new vl;return n.disableClose=!1,n.autoFocus=!0,n.exitAnimationDuration="1000ms",n.enterAnimationDuration="1000ms",n.width="90vh",n.height="80vh",n.maxWidth="100vh",n}static async generateSHA256(n){var e=new TextEncoder;let i=e.encode(n),r=await crypto.subtle.digest("SHA-256",i);return Array.from(new Uint8Array(r)).map(a=>a.toString(16).padStart(2,"0")).join("")}static isSha256(n){return/^[0-9a-fA-F]{64}$/.test(n)}};var j=class{static PRODUCTOS_ID="productoList";static USUARIOS_ID="usuarioList";static LOTES_INVENTARIO_ID="loteList";static PEDIDOS_ID="pedidoList";static DETALLE_PEDIDOS_ID="detallePedidoList";static MOVIMIENTOS_INVENTARIO_ID="movimientoInventarioList";static ESTADO_PEDIDO_ID="estadoPedidoList";static ESTADO_PRODUCTO_ID="estadoProductoList";static TIPO_MOVIMIENTO_ID="tipoMovimientoList";static PASARELA_ID="pasarelaList";static DETALLE_PEDIDOS_COMPRA_ID="detallePedidoCompraList";static LOGGED_USUARIO="usuario";static LOGGED="logged";static estadoPedidoVacio(){return{id:0,descripcion:""}}static estadoProductoVacio(){return{id:0,descripcion:""}}static tipoMovimientoVacio(){return{id:0,descripcion:"",cuenta:""}}static usuarioVacio(){return{id:0,usuario:"",password:"",nombres:"",apellidos:"",fecha_creacion:new Date}}static productoVacio(){return{id:0,sku:"",nombre:"",descripcion:"",precio_venta:0,estado:this.estadoProductoVacio()}}static loteInventarioVacio(){return{id:0,producto:this.productoVacio(),cantidad_inicial:0,cantidad_actual:0,costo_unitario:0,fecha_ingreso:new Date}}static pedidoVacio(){return{id:0,usuario:this.usuarioVacio(),total:0,estado:this.estadoPedidoVacio()}}static detallePedidoVacio(){return{id:0,pedido:this.pedidoVacio(),producto:this.productoVacio(),cantidad:0,precio_unitario_venta:0}}static detalleCompraVacio(){return{id:0,producto:this.productoVacio(),cantidad:0,cantidad_local:0,cantidad_web:0,precio_unitario_venta:0}}static movimientoInventarioVacio(){return{id:0,producto:this.productoVacio(),lote:this.loteInventarioVacio(),tipo_movimiento:this.tipoMovimientoVacio(),cantidad:0,pedido:this.pedidoVacio(),fecha:new Date}}static pasarelaVacio(){return{id:0,nombre:"",descripcion:"",comision:0}}static getUsuarios(){var n=[],e="";return ge.generateSHA256("1234").then(i=>e=i),n.push({id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}),n.push({id:2,usuario:"jamileth",password:e,nombres:"Jamileth",apellidos:"Martinez",fecha_creacion:new Date}),n.push({id:3,usuario:"rubix",password:e,nombres:"Rubi",apellidos:"Mejia",fecha_creacion:new Date}),n.push({id:4,usuario:"ale",password:e,nombres:"Alejandra",apellidos:"Guardado",fecha_creacion:new Date}),n.push({id:5,usuario:"khaysernberg",password:e,nombres:"Cesar",apellidos:"Gomez",fecha_creacion:new Date}),n.push({id:6,usuario:"miriam",password:e,nombres:"Mirian",apellidos:"Rivas",fecha_creacion:new Date}),n}static getEstadosPedido(){var n=[];return n.push({id:1,descripcion:"Carrito"}),n.push({id:2,descripcion:"Apartado"}),n.push({id:3,descripcion:"Pagado"}),n}static getEstadosProducto(){var n=[];return n.push({id:1,descripcion:"Disponible"}),n.push({id:2,descripcion:"NoDisponible"}),n.push({id:3,descripcion:"Agotado"}),n}static getTiposMovimiento(){var n=[];return n.push({id:1,descripcion:"Compra",cuenta:"410101"}),n.push({id:2,descripcion:"Venta",cuenta:"210801"}),n.push({id:3,descripcion:"Merma",cuenta:"420201"}),n.push({id:4,descripcion:"Devolucion Compra",cuenta:"520403"}),n.push({id:5,descripcion:"Devolucion Venta",cuenta:"510103"}),n}static getProductos(){var n=[],e=this.getEstadosProducto();return n.push({id:1,sku:"p1",nombre:"producto1",descripcion:"descripcion1",precio_venta:1.5,estado:e[0],stock_local:25,stock_web:125}),n.push({id:2,sku:"p2",nombre:"producto2",descripcion:"descripcion2",precio_venta:2.5,estado:e[0],stock_local:25,stock_web:50}),n.push({id:3,sku:"p3",nombre:"producto3",descripcion:"descripcion3",precio_venta:3.5,estado:e[0],stock_local:25,stock_web:125}),n}static getLotesInventario(){var n=[],e=this.getProductos();return n.push({id:1,producto:e[0],cantidad_inicial:100,cantidad_actual:100,costo_unitario:.9,fecha_ingreso:new Date}),n.push({id:2,producto:e[0],cantidad_inicial:50,cantidad_actual:50,costo_unitario:.95,fecha_ingreso:new Date}),n.push({id:3,producto:e[1],cantidad_inicial:75,cantidad_actual:75,costo_unitario:2,fecha_ingreso:new Date}),n.push({id:4,producto:e[2],cantidad_inicial:150,cantidad_actual:150,costo_unitario:2.75,fecha_ingreso:new Date}),n}static getPedidos(){var n=[],e=this.getUsuarios(),i=this.getEstadosPedido();return n.push({id:1,usuario:e[1],total:10,estado:i[0]}),n.push({id:2,usuario:e[2],total:10,estado:i[0]}),n.push({id:3,usuario:e[3],total:10,estado:i[2]}),n}static getDetallePedidos(){var n=[],e=this.getProductos(),i=this.getPedidos();return n.push({id:1,pedido:i[0],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:2,pedido:i[0],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:3,pedido:i[1],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:4,pedido:i[1],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:5,pedido:i[2],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:6,pedido:i[2],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n}static getMovimientosInventario(){var n=[],e=this.getProductos(),i=this.getLotesInventario(),r=this.getTiposMovimiento();return n.push({id:1,producto:e[0],lote:i[0],tipo_movimiento:r[0],cantidad:100,fecha:new Date}),n.push({id:2,producto:e[0],lote:i[1],tipo_movimiento:r[0],cantidad:50,fecha:new Date}),n.push({id:3,producto:e[1],lote:i[2],tipo_movimiento:r[0],cantidad:75,fecha:new Date}),n.push({id:4,producto:e[2],lote:i[3],tipo_movimiento:r[0],cantidad:150,fecha:new Date}),n}static getPasarelas(){var n=[];return n.push({id:1,nombre:"Wompi",descripcion:"wompi el salvador",comision:1.5}),n.push({id:2,nombre:"Serfinsa",descripcion:"serfinsa el salvador",comision:1.5}),n.push({id:3,nombre:"PayPal",descripcion:"PayPal international",comision:1.5}),n}};var I3=new C("WindowLocalStorage",{providedIn:"root",factory:()=>{let t=d(Ur);return zm(t)?localStorage:{length:0,clear:()=>{},getItem:()=>null,key:()=>null,removeItem:()=>{},setItem:()=>{}}}}),an=class t{storage=d(I3);setItem(n,e){let i=typeof e=="string"?e:JSON.stringify(e);this.storage.setItem(n,i)}getItem(n){let e=this.storage.getItem(n);if(!e)return null;try{return JSON.parse(e)}catch{return e}}removeItem(n){this.storage.removeItem(n)}clear(){this.storage.clear()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Je=class t{localStorageService=d(an);constructor(){}getUsuarios(){var n=[],e=this.localStorageService.getItem(j.USUARIOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getUsuarios(),this.localStorageService.setItem(j.USUARIOS_ID,n)),n}getEstadosPedido(){var n=[],e=this.localStorageService.getItem(j.ESTADO_PEDIDO_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getEstadosPedido(),this.localStorageService.setItem(j.ESTADO_PEDIDO_ID,n)),n}getEstadosProducto(){var n=[],e=this.localStorageService.getItem(j.ESTADO_PRODUCTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getEstadosProducto(),this.localStorageService.setItem(j.ESTADO_PRODUCTO_ID,n)),n}getTiposMovimiento(){var n=[],e=this.localStorageService.getItem(j.TIPO_MOVIMIENTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getTiposMovimiento(),this.localStorageService.setItem(j.TIPO_MOVIMIENTO_ID,n)),n}getProductos(){var n=[],e=this.localStorageService.getItem(j.PRODUCTOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getProductos(),this.localStorageService.setItem(j.PRODUCTOS_ID,n)),n}getLotesInventario(){var n=[],e=this.localStorageService.getItem(j.LOTES_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getLotesInventario(),this.localStorageService.setItem(j.LOTES_INVENTARIO_ID,n)),n}getPedidos(){var n=[],e=this.localStorageService.getItem(j.PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getPedidos(),this.localStorageService.setItem(j.PEDIDOS_ID,n)),n}getDetallePedidos(){var n=[],e=this.localStorageService.getItem(j.DETALLE_PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getDetallePedidos(),this.localStorageService.setItem(j.DETALLE_PEDIDOS_ID,n)),n}getMovimientosInventario(){var n=[],e=this.localStorageService.getItem(j.MOVIMIENTOS_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=j.getMovimientosInventario(),this.localStorageService.setItem(j.MOVIMIENTOS_INVENTARIO_ID,n)),n}getPasarelas(){var n=[],e=this.localStorageService.getItem(j.PASARELA_ID);return e&&(n=e,this.localStorageService.setItem(j.PASARELA_ID,n)),(!n||n.length==0)&&(n=j.getPasarelas()),n}pushProducto(n){var e=this.getProductos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(j.PRODUCTOS_ID,e)}pushLoteInventario(n){var e=this.getLotesInventario();return n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(j.LOTES_INVENTARIO_ID,e),n}pushPedido(n){var e=this.getPedidos();return n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(j.PEDIDOS_ID,e),n}pushDetallePedido(n){var e=this.getDetallePedidos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(j.DETALLE_PEDIDOS_ID,e)}pushMovimientoInventario(n){var e=this.getMovimientosInventario();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(j.MOVIMIENTOS_INVENTARIO_ID,e)}editarProducto(n){var e=this.getProductos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(j.PRODUCTOS_ID,i)}editarLoteInventario(n){var e=this.getLotesInventario(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(j.LOTES_INVENTARIO_ID,i)}editarDetallePedido(n){var e=this.getDetallePedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(j.DETALLE_PEDIDOS_ID,i)}editarPedido(n){var e=this.getPedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(j.PEDIDOS_ID,i)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Ii=class t{dataService=d(Je);_snackBar=d(Qt);constructor(){}getPedidosUsuario(n){var e=[],i=this.dataService.getPedidos();if(i.length>0)for(let r of i)r.usuario.id==n.id&&r.estado.id<3&&e.push(r);return e}getUltimoPedidoUsuario(n){var e=j.pedidoVacio(),i=this.getPedidosUsuario(n);if(i.length>0)e=i[i.length-1];else{let r=this.dataService.getEstadosPedido();e.usuario=n,e.total=0,e.estado=r[r.findIndex(o=>o.id==1)],e=this.dataService.pushPedido(e)}return e}getDetallesPedido(n){var e=[],i=this.dataService.getDetallePedidos();if(i.length>0)for(let r of i)r.pedido.id==n.id&&e.push(r);return e}aumentarDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&(i.cantidad++,this.dataService.editarDetallePedido(i));return e}disminuirDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&i.cantidad>0&&(i.cantidad--,this.dataService.editarDetallePedido(i));return e}pedidoPagado(n){var e=this.dataService.getEstadosPedido();n.estado=e[e.findIndex(r=>r.id==3)],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)r.pedido.id==n.id&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r,1))}ventaPagada(n){var e=this.dataService.getEstadosPedido();n.estado=e[e.findIndex(r=>r.id==3)],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)r.pedido.id==n.id&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r,2))}registrarPagoInventario(n,e){let i=j.movimientoInventarioVacio();i.cantidad=n.cantidad,i.fecha=new Date,i.pedido=n.pedido,i.producto=n.producto;let r=this.dataService.getTiposMovimiento();i.tipo_movimiento=r[r.findIndex(l=>l.id==2)];let o=this.dataService.getLotesInventario(),a=[],s=0;s=n.cantidad;for(let l of o)l.producto.id==n.producto.id&&l.cantidad_actual>0&&s>0&&(a.push(l),l.cantidad_actual<s&&(s-=l.cantidad_actual));s=n.cantidad;for(let l of a)if(s>0){l.cantidad_actual<s?(s-=l.cantidad_actual,l.cantidad_actual=0):(l.cantidad_actual-=s,s=0);let c=i;c.lote=l,this.dataService.editarLoteInventario(l),this.dataService.pushMovimientoInventario(c)}e==1&&n.producto.stock_web&&n.producto.stock_web>0&&(n.producto.stock_web-=n.cantidad,this.dataService.editarProducto(n.producto)),e==2&&n.producto.stock_local&&n.producto.stock_local>0&&(n.producto.stock_local-=n.cantidad,this.dataService.editarProducto(n.producto))}validarExistenciaInventario(n){let e=this.dataService.getLotesInventario(),i=[],r=0;r=n.cantidad;for(let o of e)o.producto.id==n.producto.id&&o.cantidad_actual>0&&r>0&&(i.push(o),o.cantidad_actual<r?r-=o.cantidad_actual:r=0);return i&&i.length>0&&r==0}getProductosStocWeb(){let n=this.dataService.getProductos(),e=[];if(n.length>0)for(let i of n)i.estado.id==1&&i.stock_web&&i.stock_web>0&&e.push(i);return e}getProductosStocLocal(){let n=this.dataService.getProductos(),e=[];if(n.length>0)for(let i of n)i.estado.id==1&&i.stock_local&&i.stock_local>0&&e.push(i);return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Hn=class t{formBuilder=d(th);validateFormControls(n){var e=n.valid,i=n.getRawValue();for(let r=0;r<i.length;r++)i[r]?.enabled&&!i[r]?.valid&&(e=!1);return e}getFormGroup(n){var e={};return n&&n.length>0&&n.forEach(i=>e[i.name]=[i.value||"",i.validators||[]]),this.formBuilder.group(e)}newProductoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.descripcion,value:i})),[{id:1,name:"sku",label:"Codigo",type:"text",controlType:"input",validators:[re.required,re.minLength(3)],min:3},{id:2,name:"nombre",label:"Nombre Producto",type:"text",controlType:"input",validators:[re.required,re.minLength(5)],min:5},{id:3,name:"descripcion",label:"Descripcion",type:"text",controlType:"input",validators:[re.required,re.minLength(5)],min:5},{id:4,name:"precio_venta",value:0,label:"Precio de Venta",type:"number",controlType:"input",validators:[re.required,re.min(.01)],min:.01},{id:5,name:"estado",label:"Estado",type:"text",controlType:"select",validators:[re.required],options:e}]}newCompraFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[re.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[re.required,re.min(.01)],min:.01},{id:3,name:"pagoTotal",value:0,label:"Pago Total",type:"number",controlType:"input",validators:[re.required,re.min(.01)],min:.01}]}newVentaLineaFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[re.required],options:e}]}newVentaLocalFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[re.required],options:e}]}newVentaLocalTipoPagoFormControls(){var n=[];return n.push({id:1,name:"Efectivo",value:1}),n.push({id:2,name:"Tarjeta",value:2}),[{id:1,name:"tipoPago",label:"Forma de Pago",type:"text",controlType:"select",validators:[re.required],options:n}]}newVentaLocalClienteFormControls(){return[{id:1,name:"nombre",label:"Nombre",type:"text",controlType:"input",validators:[re.required,re.minLength(3)],min:3},{id:2,name:"documento",label:"Numero Documento",type:"text",controlType:"input",validators:[re.required,re.minLength(3)],min:3}]}newAgregarDetallePedidoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", Precio: $"+i.precio_venta,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[re.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[re.required,re.min(1)],min:1}]}newAgregarDetallePedidoVentaControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", Disponible: $"+i.stock_local,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[re.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[re.required,re.min(1)],min:1}]}newAgregarDetalleCompraControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[re.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[re.required,re.min(1)],min:1},{id:3,name:"cantidad_web",value:0,label:"Cantidad Stock Web",type:"number",controlType:"input",validators:[re.required,re.min(0)],min:0},{id:4,name:"cantidad_local",value:0,label:"Cantidad Stock local",type:"number",controlType:"input",validators:[re.required,re.min(0)],min:0}]}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Un=class t{dataService=d(Je);localStorageService=d(an);constructor(){}getUsuarioAnonimo(){return{id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}}getUsuarios(){return this.dataService.getUsuarios()}pushUsuario(n){var e=this.getUsuarios();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(j.USUARIOS_ID,e)}editarUsuario(n){var e=this.getUsuarios(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n,ge.isSha256(o.password)||ge.generateSHA256(o.password).then(a=>o.password=a)),i.push(o)}),this.localStorageService.setItem(j.USUARIOS_ID,i)}validarUsuario(n,e){var i="";ge.generateSHA256(e).then(a=>i=a);var r=this.getUsuarios(),o=j.usuarioVacio();return r&&r.length&&r.filter(a=>a.usuario==n).filter(a=>a.password==i).forEach(a=>o=a),o&&o.id>1?o:null}getUsuarioLoggeado(){var n=j.usuarioVacio(),e=this.localStorageService.getItem("usuario");return e&&(n=e),n}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var zn=class{isErrorState(n,e){let i=e&&e.submitted;return!!(n&&n.invalid&&(n.dirty||n.touched||i))}};var uk=()=>["producto","cantidad","precio","opciones"];function T3(t,n){t&1&&(f(0,"th",14),g(1,"Producto"),p()),t&2&&E("text-align","center")}function A3(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),Dr(" ",e.producto.sku," ",e.producto.nombre," ")}}function R3(t,n){t&1&&(f(0,"th",14),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function k3(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad)}}function O3(t,n){t&1&&(f(0,"th",14),g(1,"Precio"),p()),t&2&&E("text-align","center")}function N3(t,n){if(t&1&&(f(0,"td",15),g(1),Wt(2,"currency"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",mi(2,3,e.precio_unitario_venta))}}function P3(t,n){t&1&&(f(0,"th",16),g(1,"OPCIONES"),p())}function F3(t,n){if(t&1){let e=ze();f(0,"td",15)(1,"button",17),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.aumentarDetallePedido(r))}),f(2,"mat-icon"),g(3,"arrow_circle_up"),p()(),f(4,"button",17),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.disminuirDetallePedido(r))}),f(5,"mat-icon"),g(6,"arrow_circle_down"),p()()()}}function L3(t,n){t&1&&U(0,"tr",18)}function V3(t,n){t&1&&U(0,"tr",19)}function j3(t,n){if(t&1&&U(0,"textarea",4),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function B3(t,n){if(t&1&&(f(0,"mat-option",8),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function H3(t,n){if(t&1){let e=ze();f(0,"mat-select",7),F("selectionChange",function(r){Ae(e);let o=y(2);return Re(o.onSelectionChange(r))}),Ze(1,B3,2,2,"mat-option",8,Qe),p()}if(t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function U3(t,n){if(t&1&&U(0,"input",6),t&2){let e=y().$implicit,i=y();x("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function z3(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=y().$implicit;h(),ve(e.label)}}function $3(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Seleccione ",e.label)}}function G3(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Ingrese ",e.label)}}function W3(t,n){if(t&1&&(f(0,"div")(1,"mat-form-field")(2,"mat-label"),g(3),p(),I(4,j3,1,2,"textarea",4)(5,H3,3,3,"mat-select",5)(6,U3,1,3,"input",6),I(7,z3,2,1,"mat-hint"),I(8,$3,2,1,"mat-error"),I(9,G3,2,1,"mat-error"),p()()),t&2){let e,i,r,o,a=n.$implicit,s=y();h(3),ve(a.label),h(),T((e=a.controlType)==="textarea"?4:e==="select"?5:6),h(3),T((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),h(),T((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),h(),T((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}function q3(t,n){if(t&1&&U(0,"textarea",4),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Y3(t,n){if(t&1&&(f(0,"mat-option",9),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function Q3(t,n){if(t&1){let e=ze();f(0,"mat-select",8),F("selectionChange",function(r){Ae(e);let o=y(2);return Re(o.onProductoSelectionChange(r))}),Ze(1,Y3,2,2,"mat-option",9,Qe),p()}if(t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function Z3(t,n){if(t&1&&U(0,"input",6),t&2){let e=y().$implicit,i=y();x("maxlength",Xn(e.max&&e.max>0&&e.type=="text"?e.max:1e3))("max",Xn(e.max&&e.max>0&&e.type=="number"?e.max:1e6))("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function K3(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Seleccione"," "+e.label)}}function X3(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Ingrese"," "+e.label)}}function J3(t,n){if(t&1&&(I(0,K3,2,1,"mat-hint",10),I(1,X3,2,1,"mat-hint",10)),t&2){let e,i,r=y().$implicit,o=y();T((e=o.getFormControl(r.name))!=null&&e.errors&&r.controlType==="select"?0:-1),h(),T((i=o.getFormControl(r.name))!=null&&i.errors&&r.controlType!="select"?1:-1)}}function e4(t,n){if(t&1&&(f(0,"mat-hint",7),g(1),p()),t&2){let e,i=y().$implicit,r=y();h(),wr("",i.min&&i.min>0?i.min:"-","/",i.type=="number"?(e=r.getFormControl(i.name))==null?null:e.value:(e=r.getFormControl(i.name))==null?null:e.value.length,"/",i.max&&i.max>0?i.max:"-")}}function t4(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),I(4,q3,1,2,"textarea",4)(5,Q3,3,3,"mat-select",5)(6,Z3,1,7,"input",6),I(7,J3,2,2),I(8,e4,2,3,"mat-hint",7),p()()),t&2){let e,i,r=n.$implicit,o=y();h(3),ve(r.label),h(),T((e=r.controlType)==="textarea"?4:e==="select"?5:6),h(3),T((i=o.getFormControl(r.name))!=null&&i.errors?7:-1),h(),T(r.controlType=="input"&&(r.max||r.min)?8:-1)}}var Eh=class t{innerWidths="0";document=d(V);dialog=d(Ki);usuarioService=d(Un);carritoService=d(Ii);usuario=S(j.usuarioVacio());pedido=S(j.pedidoVacio());detallePedidoList=S([]);constructor(){}ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=ge.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},this.dialog.open(nC,n).afterClosed().subscribe(i=>{this.cargarListas()})}agregarDetallePedido(){let n=ge.getMatDialogConf();n.data={pedido:this.pedido()},this.dialog.open(iC,n).afterClosed().subscribe(i=>{this.cargarListas()})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-carrito"]],decls:27,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Carrito de compras "),p()(),f(5,"table",2),ee(6,3),N(7,T3,2,2,"th",4)(8,A3,2,4,"td",5),te(),ee(9,6),N(10,R3,2,2,"th",4)(11,k3,2,3,"td",5),te(),ee(12,7),N(13,O3,2,2,"th",4)(14,N3,3,5,"td",5),te(),ee(15,8),N(16,P3,2,0,"th",9)(17,F3,7,0,"td",10),te(),N(18,L3,1,0,"tr",11)(19,V3,1,0,"tr",12),p()(),f(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),F("click",function(){return i.pagoCarrito()}),g(24,"Pagar Carrito"),p(),f(25,"button",13),F("click",function(){return i.agregarDetallePedido()}),g(26,"Agregar Producto"),p()()()()()),e&2&&(h(),E("overflow","auto"),h(4),x("dataSource",i.detallePedidoList()),h(13),x("matHeaderRowDef",ot(5,uk)),h(),x("matRowDefColumns",ot(6,uk)))},dependencies:[Mi,Rr,ri,oi,xi,Ar,ut,ro,Bn,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,Er],encapsulation:2})},nC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(V);_snackBar=d(Qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);dataService=d(Je);carritoService=d(Ii);usuarioService=d(Un);pagoCarritoForm;detallePedidoList=S([]);pasarelaList=S([]);totalPedido=S(0);comisionPasarela=S(0);totalPago=S(0);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLineaFormControls(this.pasarelaList());this.pagoCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.cargarDatosForm()}getFormControl(n){return this.pagoCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionChange(n){let e=n.value;this.comisionPasarela.update(i=>e.comision),this.totalPago.update(i=>this.totalPedido()+e.comision)}pagarPedido(){if(this.validarDatos())if(this.totalPago()>0&&this.totalPedido()>0){var n=this.detallePedidoList()[0].pedido;this.carritoService.pedidoPagado(n),ge.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else ge.openSnackBar("Datos Incorrectos","aceptar",this._snackBar);else ge.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.pagoCarritoForm);return n}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:32,vars:10,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),g(5,"Pagar Pedido"),p()(),f(6,"mat-card-content")(7,"form",1),F("ngSubmit",function(){return i.pagarPedido()}),f(8,"mat-grid-list",2)(9,"div")(10,"mat-label"),g(11,"Total de pedido: "),p(),f(12,"mat-label"),g(13),Wt(14,"currency"),p()(),Ze(15,W3,10,5,"div",null,Qe),f(17,"div")(18,"mat-label"),g(19,"comision pasarela: "),p(),f(20,"mat-label"),g(21),Wt(22,"currency"),p()(),f(23,"div")(24,"mat-label"),g(25,"Total a pagar: "),p(),f(26,"mat-label"),g(27),Wt(28,"currency"),p()(),f(29,"div")(30,"button",3),g(31,"Pagar"),p()()()()()()()()),e&2&&(h(7),x("formGroup",i.pagoCarritoForm),h(6),ve(mi(14,4,i.totalPedido())),h(2),Ke(i.formConfigs()),h(6),ve(mi(22,6,i.comisionPasarela())),h(6),ve(mi(28,8,i.totalPago())))},dependencies:[Si,ut,rn,er,nr,tr,bn,on,jn,Yo,ir,kr,yn,Ei,Ci,Ln,Vn,bi,ii,Yt,ni,io,Er],encapsulation:2})},iC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px"}innerWidths="0";document=d(V);_snackBar=d(Qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);carritoService=d(Ii);dataService=d(Je);usuarioService=d(Un);agregarCarritoForm;productoList=S([]);pedido=j.pedidoVacio();ngOnInit(){this.cargarDatos(),this.cargarListas();var n=this.formService.newAgregarDetallePedidoControls(this.productoList());this.agregarCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}getFormControl(n){return this.agregarCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(){var n=this.usuarioService.getUsuarioLoggeado();this.pedido=this.carritoService.getUltimoPedidoUsuario(n)}cargarListas(){this.productoList.update(n=>[...this.carritoService.getProductosStocWeb()])}onProductoSelectionChange(n){let e=n.value;if(e&&e.id>0&&e.stock_web&&e.stock_web>0){this.getFormControl("cantidad")?.setValidators([re.required,re.min(1),re.max(e.stock_web)]);var i=this.formConfigs();i[i.findIndex(r=>r.name=="cantidad")].max=e.stock_web,this.formConfigs.update(r=>[...i])}}agregarDetallePedido(){if(this.validarDatos()){let n=this.agregarCarritoForm.value,e=j.detallePedidoVacio();e.pedido=this.pedido,e.cantidad=n.cantidad,e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.dataService.pushDetallePedido(e),ge.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else ge.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCarritoForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","maxlength","max","errorStateMatcher"],["align","end"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"],[3,"value"],["align","start"]],template:function(e,i){e&1&&(g(0,"3"),f(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),g(6,"Agregar Producto"),p()(),f(7,"mat-card-content")(8,"form",1),F("ngSubmit",function(){return i.agregarDetallePedido()}),f(9,"mat-grid-list",2),Ze(10,t4,9,4,"mat-card-title",null,Qe),f(12,"mat-card-title")(13,"button",3),g(14,"Agregar"),p()()()()()()()()),e&2&&(h(8),x("formGroup",i.agregarCarritoForm),h(2),Ke(i.formConfigs()))},dependencies:[Si,ut,rn,er,nr,tr,bn,on,jn,ir,kr,yn,Ei,Ci,Ln,Vn,bi,ii,no,Yt,ni,io],encapsulation:2})};var Rl=class t{documento=d(V);localStorageService=d(an);theme=S("light");constructor(){Vi(()=>{let n=this.theme;this.localStorageService.setItem("app-theme",n()?n():"light");let e=this.documento.documentElement;n()==="dark"?(e.classList.add("dark"),e.classList.remove("light")):(e.classList.add("light"),e.classList.remove("dark"))})}toggleTheme(){this.theme.update(n=>n==="light"?"dark":"light")}setTheme(n){this.theme.set(n)}getTheme(){return this.localStorageService.getItem("app-theme")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};function n4(t,n){t&1&&(f(0,"mat-icon"),g(1,"account_circle"),p())}function i4(t,n){t&1&&(f(0,"mat-icon"),g(1,"menu"),p())}function r4(t,n){if(t&1&&(f(0,"button",3),g(1),p()),t&2){let e=n.$implicit;x("routerLink",e.ruta),h(),ve(e.nombre)}}function o4(t,n){if(t&1){let e=ze();f(0,"button",9),F("click",function(){Ae(e);let r=y();return Re(r.cerrarSesion())}),f(1,"mat-icon"),g(2,"account_circle_off"),p(),f(3,"span"),g(4,"Cerrar Sesion"),p()(),U(5,"mat-divider")}if(t&2){let e=y();x("hidden",e.logged()==!1)}}var Sh=class t{logged=S(!1);isChecked=S(!1);isLightTheme=S(!0);localStorage=d(an);themeService=d(Rl);_router=d(vn);menusList=S([]);constructor(){}ngOnInit(){this.cargarMenusDeMantenimientos(),this.validarUsuarioLogeado();var n=this.themeService.getTheme();this.isLightTheme.update(e=>n=="light")}onThemeSwitchChange(){this.themeService.toggleTheme()}routerActivated(n){this.validarUsuarioLogeado()}cargarMenusDeMantenimientos(){var n=[];n.push({id:1,ruta:"/menu/login",nombre:"Iniciar Sesion"}),n.push({id:2,ruta:"/menu/productos",nombre:"Productos"}),n.push({id:3,ruta:"/menu/carrito",nombre:"Carrito"}),n.push({id:4,ruta:"/menu/ventas",nombre:"Ventas"}),n.push({id:5,ruta:"/menu/compras",nombre:"Compras"}),n.push({id:6,ruta:"/menu/inventario",nombre:"Inventario"}),this.menusList.update(e=>[...n])}homeClick(){let n={queryParams:{nada:"xd"}};this._router.navigate(["/menu/productos"],n)}cerrarSesion(){let n={queryParams:{logged:"false"}};this.localStorage.setItem(j.LOGGED,"false"),this._router.navigate(["/menu/login"],n)}limpiarDatos(){this.localStorage.clear()}validarUsuarioLogeado(){let n=this.localStorage.getItem(j.LOGGED);this.logged.update(e=>n==!0)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-inicio"]],decls:25,vars:14,consts:[["menu","matMenu"],["matIconButton","",3,"matMenuTriggerFor"],["src","logo5.png",3,"click"],["mat-menu-item","","routerLinkActive","active-menu-item",3,"routerLink"],["mat-menu-item","",3,"click"],[3,"ngModelChange","change","ngModel"],[1,"container-fluid"],[1,"row"],[3,"activate"],["mat-menu-item","",3,"click","hidden"]],template:function(e,i){if(e&1){let r=ze();f(0,"mat-toolbar")(1,"button",1),I(2,n4,2,0,"mat-icon"),I(3,i4,2,0,"mat-icon"),p(),U(4,"span"),f(5,"span")(6,"img",2),F("click",function(){return i.homeClick()}),p()(),U(7,"span"),p(),f(8,"mat-menu",null,0),Ze(10,r4,2,2,"button",3,Qe),U(12,"mat-divider"),I(13,o4,6,1),f(14,"button",4),F("click",function(){return i.limpiarDatos()}),f(15,"mat-icon"),g(16,"clear"),p(),f(17,"span"),g(18,"Limpiar Datos"),p()(),U(19,"mat-divider"),f(20,"mat-slide-toggle",5),Dm("ngModelChange",function(a){return Ae(r),cy(i.isChecked,a)||(i.isChecked=a),Re(a)}),F("change",function(){return i.onThemeSwitchChange()}),g(21),p()(),f(22,"div",6)(23,"div",7)(24,"router-outlet",8),F("activate",function(a){return i.routerActivated(a)}),p()()()}if(e&2){let r=$t(9);h(),x("matMenuTriggerFor",r),h(),T(i.logged()?2:-1),h(),T(i.logged()?-1:3),h(),E("margin-left","1em"),h(2),E("height","100%")("max-height","2em"),h(),E("margin-left","1em"),h(3),Ke(i.menusList()),h(3),T(i.logged()?13:-1),h(7),Cm("ngModel",i.isChecked),h(),z("Colores: ",i.isChecked()?"Oscuro":"Claro")}},dependencies:[xh,ck,Bn,Il,Zd,$R,j0,Vn,d0,ja,io,KA,sl,Ib,Wo,ro],encapsulation:2})};function a4(t,n){if(t&1&&(f(0,"label",1),g(1),p()),t&2){let e=y();h(),ve(e.mensajeLogin)}}function s4(t,n){t&1&&(f(0,"mat-error"),g(1,"Ingrese Usuario"),p())}function l4(t,n){t&1&&(f(0,"mat-error"),g(1,"Ingrese Contrase\xF1a"),p())}var Mh=class t{constructor(n,e){this._router=n;this.formBuilder=e;this.loginForm=this.formBuilder.group({username:["",[re.required,re.minLength(3)]],password:["",[re.required,re.minLength(3)]]})}loginForm;mensajeLogin="";_snackBar=d(Qt);usuarioService=d(Un);localStorage=d(an);matcher=new zn;login(){if(this.validarDatos()){var n=this.usuarioService.validarUsuario(this.loginF("username")?.value,this.loginF("password")?.value);if(n&&n.id>1){ge.openSnackBar("Login exitoso","ok",this._snackBar),this.localStorage.setItem(j.LOGGED_USUARIO,n),this.localStorage.setItem(j.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}else ge.openSnackBar("Credenciales incorrectas","ok",this._snackBar)}else ge.openSnackBar("Completar credenciales","ok",this._snackBar)}iniciarInvitado(){var n=this.usuarioService.getUsuarioAnonimo();this.localStorage.setItem(j.LOGGED_USUARIO,n),this.localStorage.setItem(j.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}validarDatos(){var n=this.loginForm.valid;return this.loginF("username")?.valid||(n=!1),this.loginF("password")?.valid||(n=!1),n}loginF(n){return this.loginForm.get(n)}static \u0275fac=function(e){return new(e||t)(J(vn),J(th))};static \u0275cmp=O({type:t,selectors:[["app-login"]],decls:28,vars:6,consts:[["hideToggle","","expanded","true",1,"login-container"],[1,"h4"],["id","loginForm",3,"ngSubmit","formGroup"],["cols","1"],["matInput","","required","","formControlName","username",3,"errorStateMatcher"],["matInput","","required","","formControlName","password","type","password",3,"errorStateMatcher"],["mat-stroked-button","","color","primary","type","submit"],["matButton","",3,"click"]],template:function(e,i){if(e&1&&(f(0,"mat-expansion-panel",0)(1,"mat-expansion-panel-header")(2,"mat-panel-title"),g(3," Iniciar Sesi\xF3n "),p()(),f(4,"mat-card")(5,"mat-card-title"),I(6,a4,2,1,"label",1),p(),f(7,"mat-card-content")(8,"form",2),F("ngSubmit",function(){return i.login()}),f(9,"mat-grid-list",3)(10,"mat-card-title")(11,"mat-form-field")(12,"mat-label"),g(13,"Usuario"),p(),U(14,"input",4),I(15,s4,2,0,"mat-error"),p()(),f(16,"mat-card-title")(17,"mat-form-field")(18,"mat-label"),g(19,"Contrase\xF1a"),p(),U(20,"input",5),I(21,l4,2,0,"mat-error"),p()(),f(22,"mat-card-title")(23,"button",6),g(24,"Ingresar"),p()()()()()(),f(25,"mat-action-row")(26,"button",7),F("click",function(){return i.iniciarInvitado()}),g(27,"Continuar como invitado"),p()()()),e&2){let r,o;h(6),T(i.mensajeLogin&&i.mensajeLogin!=""?6:-1),h(2),x("formGroup",i.loginForm),h(6),x("errorStateMatcher",i.matcher),h(),T((r=i.loginF("username"))!=null&&r.errors?15:-1),h(5),x("errorStateMatcher",i.matcher),h(),T((o=i.loginF("password"))!=null&&o.errors?21:-1)}},dependencies:[wh,ri,kR,oi,xi,ut,er,nr,tr,bn,on,Yo,ir,Ei,Ci,Ln,Vn,bi,ii,Yt,ni],encapsulation:2})};var fk=()=>["sku","nombre","descripcion","precio_venta","estado","opciones"];function c4(t,n){if(t&1){let e=ze();f(0,"button",16),F("click",function(){Ae(e);let r=y();return Re(r.crearProducto())}),g(1,"Crear Producto"),p()}}function d4(t,n){if(t&1){let e=ze();f(0,"button",16),F("click",function(){Ae(e);let r=y();return Re(r.crearProducto())}),g(1,"Editar Producto"),p()}}function u4(t,n){t&1&&(f(0,"th",17),g(1,"Codigo"),p()),t&2&&E("text-align","center")}function f4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.sku," ")}}function m4(t,n){t&1&&(f(0,"th",17),g(1,"Nombre"),p()),t&2&&E("text-align","center")}function p4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.nombre)}}function h4(t,n){t&1&&(f(0,"th",17),g(1,"Descripcion"),p()),t&2&&E("text-align","center")}function g4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.descripcion)}}function v4(t,n){t&1&&(f(0,"th",17),g(1,"Precio de Venta"),p()),t&2&&E("text-align","center")}function _4(t,n){if(t&1&&(f(0,"td",18),g(1),Wt(2,"currency"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",mi(2,3,e.precio_venta)," ")}}function y4(t,n){t&1&&(f(0,"th",17),g(1,"Periodo"),p()),t&2&&E("text-align","center")}function b4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.estado.descripcion," ")}}function C4(t,n){t&1&&(f(0,"th",19),g(1,"Editar"),p())}function D4(t,n){if(t&1){let e=ze();f(0,"td",18)(1,"button",20),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.editarProducto(r))}),f(2,"mat-icon"),g(3,"edit"),p()()()}}function w4(t,n){t&1&&U(0,"tr",21)}function x4(t,n){if(t&1){let e=ze();f(0,"tr",22),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.seleccionarProducto(r))}),p()}}function E4(t,n){t&1&&(f(0,"label",1),g(1,"Agregar Producto"),p())}function S4(t,n){t&1&&(f(0,"label",1),g(1,"Editar Producto"),p())}function M4(t,n){if(t&1&&U(0,"textarea",5),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function I4(t,n){if(t&1&&(f(0,"mat-option",9),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function T4(t,n){if(t&1&&(f(0,"mat-select",6),Ze(1,I4,2,2,"mat-option",9,Qe),p()),t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function A4(t,n){if(t&1&&U(0,"input",7,0),t&2){let e=y().$implicit,i=y();x("maxlength",Xn(e.max&&e.max>0&&e.type=="text"?e.max:1e3))("max",Xn(e.max&&e.max>0&&e.type=="number"?e.max:1e6))("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function R4(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Seleccione"," "+e.label)}}function k4(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Ingrese"," "+e.label)}}function O4(t,n){if(t&1&&(I(0,R4,2,1,"mat-hint",10),I(1,k4,2,1,"mat-hint",10)),t&2){let e,i,r=y().$implicit,o=y();T((e=o.getFormControl(r.name))!=null&&e.errors&&r.controlType==="select"?0:-1),h(),T((i=o.getFormControl(r.name))!=null&&i.errors&&r.controlType!="select"?1:-1)}}function N4(t,n){if(t&1&&(f(0,"mat-hint",8),g(1),p()),t&2){let e,i=y().$implicit,r=y();h(),wr("",i.min&&i.min>0?i.min:"-","/",i.type=="number"?(e=r.getFormControl(i.name))==null?null:e.value:(e=r.getFormControl(i.name))==null?null:e.value.length,"/",i.max&&i.max>0?i.max:"-")}}function P4(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),I(4,M4,1,2,"textarea",5)(5,T4,3,3,"mat-select",6)(6,A4,2,7,"input",7),I(7,O4,2,2),I(8,N4,2,3,"mat-hint",8),p()()),t&2){let e,i,r=n.$implicit,o=y();h(3),ve(r.label),h(),T((e=r.controlType)==="textarea"?4:e==="select"?5:6),h(3),T((i=o.getFormControl(r.name))!=null&&i.errors?7:-1),h(),T(r.controlType=="input"&&(r.max||r.min)?8:-1)}}function F4(t,n){t&1&&(f(0,"button",4),g(1,"Agregar"),p())}function L4(t,n){t&1&&(f(0,"button",4),g(1,"Editar"),p())}var Ih=class t{innerWidths="0";document=d(V);dialog=d(Ki);dataService=d(Je);productoSeleccionado=j.productoVacio();productosList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.productosList.update(()=>[...this.dataService.getProductos()])}seleccionarProducto(n){this.productoSeleccionado=n}crearProducto(){let n=ge.getMatDialogConf();n.data={productoSeleccionado:this.productoSeleccionado},this.dialog.open(rC,n).afterClosed().subscribe(i=>{this.cargarListas(),this.productoSeleccionado=j.productoVacio()})}editarProducto(n){this.productoSeleccionado=n,this.crearProducto()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-productos"]],decls:31,vars:9,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-stroked-button","","color","primary"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","nombre"],["matColumnDef","descripcion"],["matColumnDef","precio_venta"],["matColumnDef","estado"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",3,"click",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matMiniFab","",3,"click"],["mat-header-row",""],["mat-row","",3,"click"]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-description"),I(4,c4,2,0,"button",2),I(5,d4,2,0,"button",2),p()()(),f(6,"mat-expansion-panel",1)(7,"mat-expansion-panel-header")(8,"mat-panel-title"),g(9," Tabla Productos "),p()(),f(10,"table",3),ee(11,4),N(12,u4,2,2,"th",5)(13,f4,2,3,"td",6),te(),ee(14,7),N(15,m4,2,2,"th",5)(16,p4,2,3,"td",6),te(),ee(17,8),N(18,h4,2,2,"th",5)(19,g4,2,3,"td",6),te(),ee(20,9),N(21,v4,2,2,"th",5)(22,_4,3,5,"td",6),te(),ee(23,10),N(24,y4,2,2,"th",5)(25,b4,2,3,"td",6),te(),ee(26,11),N(27,C4,2,0,"th",12)(28,D4,4,0,"td",13),te(),N(29,w4,1,0,"tr",14)(30,x4,1,0,"tr",15),p()()()),e&2&&(h(4),T(!i.productoSeleccionado||i.productoSeleccionado.id==0?4:-1),h(),T(!i.productoSeleccionado||i.productoSeleccionado.id>0?5:-1),h(),E("overflow","auto"),h(4),x("dataSource",i.productosList()),h(19),x("matHeaderRowDef",ot(7,fk)),h(),x("matRowDefColumns",ot(8,fk)))},dependencies:[Mi,Rr,ri,oi,xi,Ar,ut,rh,Bn,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,Er],encapsulation:2})},rC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.seleccionarProducto(n.productoSeleccionado)}innerWidths="0";document=d(V);_snackBar=d(Qt);dataService=d(Je);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);productoForm;productoSeleccionado=j.productoVacio();estadoProductoList=S([]);ngOnInit(){this.cargarListas();var n=this.formService.newProductoControls(this.estadoProductoList());this.productoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.seleccionarProductoForm()}getFormControl(n){return this.productoForm.get(n)}compareIds(n,e){return n.id==e.id}seleccionarProducto(n){this.productoSeleccionado=n}seleccionarProductoForm(){this.getFormControl("id")?.setValue(this.productoSeleccionado.id),this.getFormControl("sku")?.setValue(this.productoSeleccionado.sku),this.getFormControl("nombre")?.setValue(this.productoSeleccionado.nombre),this.getFormControl("descripcion")?.setValue(this.productoSeleccionado.descripcion),this.getFormControl("precio_venta")?.setValue(this.productoSeleccionado.precio_venta),this.getFormControl("estado")?.setValue(this.productoSeleccionado.estado)}cargarListas(){this.estadoProductoList.update(n=>[...this.dataService.getEstadosProducto()])}guardarProducto(){if(this.validarDatos()){if(this.productoSeleccionado.id>0){var n=this.productoSeleccionado.id;this.productoSeleccionado=this.productoForm.value,this.productoSeleccionado.id=n,this.dataService.editarProducto(this.productoSeleccionado),ge.openSnackBar("CAMBIOS GUARDADOS EXITOSAMENTE","aceptar",this._snackBar)}else this.productoSeleccionado=this.productoForm.value,this.dataService.pushProducto(this.productoSeleccionado),ge.openSnackBar("NUEVO PRODUCTO CREADO EXITOSAMENTE","aceptar",this._snackBar);this.dialogRef.close()}else ge.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.productoForm),e=parseFloat(this.getFormControl("precio_venta")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("precio_venta")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:16,vars:5,consts:[["input",""],[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","maxlength","max","errorStateMatcher"],["align","end"],[3,"value"],["align","start"]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",1),g(5,"Formulario"),p(),I(6,E4,2,0,"label",1),I(7,S4,2,0,"label",1),p(),f(8,"mat-card-content")(9,"form",2),F("ngSubmit",function(){return i.guardarProducto()}),f(10,"mat-grid-list",3),Ze(11,P4,9,4,"mat-card-title",null,Qe),f(13,"mat-card-title"),I(14,F4,2,0,"button",4),I(15,L4,2,0,"button",4),p()()()()()()()),e&2&&(h(6),T(!i.productoSeleccionado||i.productoSeleccionado.id==0?6:-1),h(),T(i.productoSeleccionado&&i.productoSeleccionado.id>0?7:-1),h(2),x("formGroup",i.productoForm),h(2),Ke(i.formConfigs()),h(3),T(!i.productoSeleccionado||i.productoSeleccionado.id==0?14:-1),h(),T(i.productoSeleccionado&&i.productoSeleccionado.id>0?15:-1))},dependencies:[Si,ut,rn,er,nr,tr,bn,on,jn,ir,kr,yn,Ei,Ci,Ln,Vn,bi,ii,no,Yt,ni],encapsulation:2})};var mk=()=>["producto","cantidad","precio","opciones"];function V4(t,n){t&1&&(f(0,"th",14),g(1,"Producto"),p()),t&2&&E("text-align","center")}function j4(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),Dr(" ",e.producto.sku," ",e.producto.nombre," ")}}function B4(t,n){t&1&&(f(0,"th",14),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function H4(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad)}}function U4(t,n){t&1&&(f(0,"th",14),g(1,"Precio"),p()),t&2&&E("text-align","center")}function z4(t,n){if(t&1&&(f(0,"td",15),g(1),Wt(2,"currency"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",mi(2,3,e.precio_unitario_venta))}}function $4(t,n){t&1&&(f(0,"th",16),g(1,"OPCIONES"),p())}function G4(t,n){if(t&1){let e=ze();f(0,"td",15)(1,"button",17),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.aumentarDetallePedido(r))}),f(2,"mat-icon"),g(3,"arrow_circle_up"),p()(),f(4,"button",17),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.disminuirDetallePedido(r))}),f(5,"mat-icon"),g(6,"arrow_circle_down"),p()()()}}function W4(t,n){t&1&&U(0,"tr",18)}function q4(t,n){t&1&&U(0,"tr",19)}function Y4(t,n){t&1&&g(0,"Datos Cliente")}function Q4(t,n){if(t&1&&U(0,"textarea",8),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Z4(t,n){if(t&1&&(f(0,"mat-option",11),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function K4(t,n){if(t&1&&(f(0,"mat-select",9),Ze(1,Z4,2,2,"mat-option",11,Qe),p()),t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function X4(t,n){if(t&1&&U(0,"input",10),t&2){let e=y().$implicit,i=y();x("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function J4(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=y().$implicit;h(),ve(e.label)}}function e5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Seleccione ",e.label)}}function t5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Ingrese ",e.label)}}function n5(t,n){if(t&1&&(f(0,"mat-form-field")(1,"mat-label"),g(2),p(),I(3,Q4,1,2,"textarea",8)(4,K4,3,3,"mat-select",9)(5,X4,1,3,"input",10),I(6,J4,2,1,"mat-hint"),I(7,e5,2,1,"mat-error"),I(8,t5,2,1,"mat-error"),p()),t&2){let e,i,r,o,a=n.$implicit,s=y();h(2),ve(a.label),h(),T((e=a.controlType)==="textarea"?3:e==="select"?4:5),h(3),T((i=s.getFormControlCliente(a.name))!=null&&i.errors?6:-1),h(),T((r=s.getFormControlCliente(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),h(),T((o=s.getFormControlCliente(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function i5(t,n){t&1&&g(0,"Tipo Pago")}function r5(t,n){if(t&1&&U(0,"textarea",8),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function o5(t,n){if(t&1&&(f(0,"mat-option",11),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function a5(t,n){if(t&1){let e=ze();f(0,"mat-select",12),F("selectionChange",function(r){Ae(e);let o=y(2);return Re(o.onSelectionTipoPagoChange(r))}),Ze(1,o5,2,2,"mat-option",11,Qe),p()}if(t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function s5(t,n){if(t&1&&U(0,"input",10),t&2){let e=y().$implicit,i=y();x("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function l5(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=y().$implicit;h(),ve(e.label)}}function c5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Seleccione ",e.label)}}function d5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Ingrese ",e.label)}}function u5(t,n){if(t&1&&(f(0,"mat-form-field")(1,"mat-label"),g(2),p(),I(3,r5,1,2,"textarea",8)(4,a5,3,3,"mat-select",9)(5,s5,1,3,"input",10),I(6,l5,2,1,"mat-hint"),I(7,c5,2,1,"mat-error"),I(8,d5,2,1,"mat-error"),p()),t&2){let e,i,r,o,a=n.$implicit,s=y();h(2),ve(a.label),h(),T((e=a.controlType)==="textarea"?3:e==="select"?4:5),h(3),T((i=s.getFormControlTipoPago(a.name))!=null&&i.errors?6:-1),h(),T((r=s.getFormControlTipoPago(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),h(),T((o=s.getFormControlTipoPago(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function f5(t,n){t&1&&g(0,"Tipo Pago")}function m5(t,n){if(t&1&&U(0,"textarea",8),t&2){let e=y().$implicit,i=y(2);x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function p5(t,n){if(t&1&&(f(0,"mat-option",11),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function h5(t,n){if(t&1&&(f(0,"mat-select",9),Ze(1,p5,2,2,"mat-option",11,Qe),p()),t&2){let e=y().$implicit,i=y(2);x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function g5(t,n){if(t&1&&U(0,"input",10),t&2){let e=y().$implicit,i=y(2);x("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function v5(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=y().$implicit;h(),ve(e.label)}}function _5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Seleccione ",e.label)}}function y5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=y().$implicit;h(),z("Ingrese ",e.label)}}function b5(t,n){if(t&1&&(f(0,"mat-form-field")(1,"mat-label"),g(2),p(),I(3,m5,1,2,"textarea",8)(4,h5,3,3,"mat-select",9)(5,g5,1,3,"input",10),I(6,v5,2,1,"mat-hint"),I(7,_5,2,1,"mat-error"),I(8,y5,2,1,"mat-error"),p()),t&2){let e,i,r,o,a=n.$implicit,s=y(2);h(2),ve(a.label),h(),T((e=a.controlType)==="textarea"?3:e==="select"?4:5),h(3),T((i=s.getFormControlTipoPago(a.name))!=null&&i.errors?6:-1),h(),T((r=s.getFormControlTipoPago(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),h(),T((o=s.getFormControlTipoPago(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function C5(t,n){if(t&1&&(f(0,"mat-step",2)(1,"form",3),N(2,f5,1,0,"ng-template",4),Ze(3,b5,9,5,"mat-form-field",null,Qe),f(5,"div")(6,"button",6),g(7,"Regresar"),p(),f(8,"button",5),g(9,"Siguiente"),p()()()()),t&2){let e=y();x("stepControl",e.datosTarjetaForm),h(),x("formGroup",e.datosTarjetaForm),h(2),Ke(e.formConfigsTarjeta())}}function D5(t,n){t&1&&g(0,"Done")}var sC=".fade-in[_ngcontent-%COMP%]{opacity:0;transform:translateY(10px);transition:opacity .4s ease,transform .4s ease}div[_ngcontent-%COMP%]{opacity:1;transform:translateY(0)}.fade-out[_ngcontent-%COMP%]{opacity:1;transform:translateY(0);transition:opacity .4s ease,transform .4s ease}.fade-out.leave-active[_ngcontent-%COMP%]{opacity:0;transform:translateY(-10px)}";function w5(t,n){if(t&1&&U(0,"textarea",4),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function x5(t,n){if(t&1&&(f(0,"mat-option",9),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function E5(t,n){if(t&1){let e=ze();f(0,"mat-select",8),F("selectionChange",function(r){Ae(e);let o=y(2);return Re(o.onProductoSelectionChange(r))}),Ze(1,x5,2,2,"mat-option",9,Qe),p()}if(t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function S5(t,n){if(t&1&&U(0,"input",6),t&2){let e=y().$implicit,i=y();x("maxlength",Xn(e.max&&e.max>0&&e.type=="text"?e.max:1e3))("max",Xn(e.max&&e.max>0&&e.type=="number"?e.max:1e6))("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function M5(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Seleccione"," "+e.label)}}function I5(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Ingrese"," "+e.label)}}function T5(t,n){if(t&1&&(I(0,M5,2,1,"mat-hint",10),I(1,I5,2,1,"mat-hint",10)),t&2){let e,i,r=y().$implicit,o=y();T((e=o.getFormControl(r.name))!=null&&e.errors&&r.controlType==="select"?0:-1),h(),T((i=o.getFormControl(r.name))!=null&&i.errors&&r.controlType!="select"?1:-1)}}function A5(t,n){if(t&1&&(f(0,"mat-hint",7),g(1),p()),t&2){let e,i=y().$implicit,r=y();h(),wr("",i.min&&i.min>0?i.min:"-","/",i.type=="number"?(e=r.getFormControl(i.name))==null?null:e.value:(e=r.getFormControl(i.name))==null?null:e.value.length,"/",i.max&&i.max>0?i.max:"-")}}function R5(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),I(4,w5,1,2,"textarea",4)(5,E5,3,3,"mat-select",5)(6,S5,1,7,"input",6),I(7,T5,2,2),I(8,A5,2,3,"mat-hint",7),p()()),t&2){let e,i,r=n.$implicit,o=y();h(3),ve(r.label),h(),T((e=r.controlType)==="textarea"?4:e==="select"?5:6),h(3),T((i=o.getFormControl(r.name))!=null&&i.errors?7:-1),h(),T(r.controlType=="input"&&(r.max||r.min)?8:-1)}}var Th=class t{constructor(n){this._router=n}_snackBar=d(Qt);innerWidths="0";document=d(V);dialog=d(Ki);usuarioService=d(Un);carritoService=d(Ii);usuario=S(j.usuarioVacio());pedido=S(j.pedidoVacio());detallePedidoList=S([]);ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=ge.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},this.dialog.open(oC,n).afterClosed().subscribe(i=>{this.cargarListas()})}agregarDetallePedido(){if(this.usuario()&&this.usuario().id>0){let n=ge.getMatDialogConf();n.data={pedido:this.pedido()},this.dialog.open(aC,n).afterClosed().subscribe(i=>{this.cargarListas()})}else{ge.openSnackBar("Usuario no registrado","aceptar",this._snackBar);let n={queryParams:{logged:"true"}};this._router.navigate(["/menu/login"],n)}}static \u0275fac=function(e){return new(e||t)(J(vn))};static \u0275cmp=O({type:t,selectors:[["app-ventas"]],decls:27,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Ventas "),p()(),f(5,"table",2),ee(6,3),N(7,V4,2,2,"th",4)(8,j4,2,4,"td",5),te(),ee(9,6),N(10,B4,2,2,"th",4)(11,H4,2,3,"td",5),te(),ee(12,7),N(13,U4,2,2,"th",4)(14,z4,3,5,"td",5),te(),ee(15,8),N(16,$4,2,0,"th",9)(17,G4,7,0,"td",10),te(),N(18,W4,1,0,"tr",11)(19,q4,1,0,"tr",12),p()(),f(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),F("click",function(){return i.pagoCarrito()}),g(24,"Registrar Venta"),p(),f(25,"button",13),F("click",function(){return i.agregarDetallePedido()}),g(26,"Agregar Producto"),p()()()()()),e&2&&(h(),E("overflow","auto"),h(4),x("dataSource",i.detallePedidoList()),h(13),x("matHeaderRowDef",ot(5,mk)),h(),x("matRowDefColumns",ot(6,mk)))},dependencies:[Mi,Rr,ri,oi,xi,Ar,ut,ro,Bn,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,Er],styles:[sC]})},oC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(V);_snackBar=d(Qt);dialogRef=d(Vt);formConfigsCliente=S([]);formConfigsTipoPago=S([]);formConfigsTarjeta=S([]);matcher=new zn;formService=d(Hn);dataService=d(Je);carritoService=d(Ii);usuarioService=d(Un);datosClienteForm;datosTarjetaForm;tipoPagoForm;detallePedidoList=S([]);pasarelaList=S([]);totalPedido=S(0);comisionPasarela=S(0);totalPago=S(0);esPagoTarjeta=S(!1);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLocalClienteFormControls(),e=this.formService.newVentaLocalTipoPagoFormControls(),i=this.formService.newVentaLocalFormControls(this.pasarelaList());this.datosClienteForm=this.formService.getFormGroup(n),this.tipoPagoForm=this.formService.getFormGroup(e),this.datosTarjetaForm=this.formService.getFormGroup(i),this.formConfigsCliente.update(r=>[...n]),this.formConfigsTipoPago.update(r=>[...e]),this.formConfigsTarjeta.update(r=>[...i]),this.cargarDatosForm()}getFormControlCliente(n){return this.datosClienteForm.get(n)}getFormControlTipoPago(n){return this.tipoPagoForm.get(n)}getFormControlTarjeta(n){return this.datosTarjetaForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionTipoPagoChange(n){let e=n.value;console.log("selected val"),console.log(e),e==2&&this.esPagoTarjeta.update(i=>!0),e==1&&this.esPagoTarjeta.update(i=>!1)}pagarPedido(){let n=!1;if(this.validarDatosCliente())if(this.validarDatosFormaPago())if(this.esPagoTarjeta()?this.validarDatosTarjeta()?n=!0:ge.openSnackBar("Datos de tarjeta incorrectos","aceptar",this._snackBar):n=!0,n&&this.totalPago()>0&&this.totalPedido()>0){var e=this.detallePedidoList()[0].pedido;this.carritoService.ventaPagada(e),ge.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else ge.openSnackBar("Datos de cliente incorrectos","aceptar",this._snackBar);else ge.openSnackBar("Datos de forma de pago incorrectos","aceptar",this._snackBar);else ge.openSnackBar("Datos de cliente incorrectos","ok",this._snackBar)}validarDatosCliente(){var n=this.formService.validateFormControls(this.datosClienteForm);return n}validarDatosFormaPago(){var n=this.formService.validateFormControls(this.tipoPagoForm);return n}validarDatosTarjeta(){var n=this.formService.validateFormControls(this.datosTarjetaForm);return n}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:34,vars:5,consts:[["stepper",""],["orientation","vertical","linear","true"],[3,"stepControl"],[3,"formGroup"],["matStepLabel",""],["matButton","","matStepperNext",""],["matButton","","matStepperPrevious",""],["matButton","",3,"click"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"]],template:function(e,i){if(e&1){let r=ze();f(0,"mat-dialog-content")(1,"mat-stepper",1,0)(3,"mat-step",2)(4,"form",3),N(5,Y4,1,0,"ng-template",4),Ze(6,n5,9,5,"mat-form-field",null,Qe),f(8,"div")(9,"button",5),g(10,"Siguiente"),p()()()(),f(11,"mat-step",2)(12,"form",3),N(13,i5,1,0,"ng-template",4),Ze(14,u5,9,5,"mat-form-field",null,Qe),f(16,"div")(17,"button",6),g(18,"Regresar"),p(),f(19,"button",5),g(20,"Siguiente"),p()()()(),I(21,C5,10,2,"mat-step",2),f(22,"mat-step"),N(23,D5,1,0,"ng-template",4),f(24,"p"),g(25,"Pagar Pedido"),p(),f(26,"div")(27,"button",7),F("click",function(){return i.pagarPedido()}),g(28,"Pagar"),p()(),f(29,"div")(30,"button",6),g(31,"Regresar"),p(),f(32,"button",7),F("click",function(){Ae(r);let a=$t(2);return Re(a.reset())}),g(33,"Reiniciar"),p()()()()()}e&2&&(h(3),x("stepControl",i.datosClienteForm),h(),x("formGroup",i.datosClienteForm),h(2),Ke(i.formConfigsCliente()),h(5),x("stepControl",i.tipoPagoForm),h(),x("formGroup",i.tipoPagoForm),h(2),Ke(i.formConfigsTipoPago()),h(7),T(i.esPagoTarjeta()?21:-1))},dependencies:[Si,ut,rn,bn,on,jn,Yo,kr,yn,Ei,Ci,Ln,Vn,bi,ii,Yt,ni,lk,J0,Jd,eC,ak,sk],styles:[sC]})},aC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px"}innerWidths="0";document=d(V);_snackBar=d(Qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);carritoService=d(Ii);dataService=d(Je);usuarioService=d(Un);agregarCarritoForm;productoList=S([]);pedido=j.pedidoVacio();ngOnInit(){this.cargarDatos(),this.cargarListas();var n=this.formService.newAgregarDetallePedidoControls(this.productoList());this.agregarCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}getFormControl(n){return this.agregarCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(){var n=this.usuarioService.getUsuarioLoggeado();this.pedido=this.carritoService.getUltimoPedidoUsuario(n)}cargarListas(){this.productoList.update(n=>[...this.carritoService.getProductosStocLocal()])}onProductoSelectionChange(n){let e=n.value;if(e&&e.id>0&&e.stock_local&&e.stock_local>0){this.getFormControl("cantidad")?.setValidators([re.required,re.min(1),re.max(e.stock_local)]);var i=this.formConfigs();i[i.findIndex(r=>r.name=="cantidad")].max=e.stock_local,this.formConfigs.update(r=>[...i])}}agregarDetallePedido(){if(this.validarDatos()){let n=this.agregarCarritoForm.value,e=j.detallePedidoVacio();e.pedido=this.pedido,e.cantidad=n.cantidad,e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.dataService.pushDetallePedido(e),ge.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else ge.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCarritoForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","maxlength","max","errorStateMatcher"],["align","end"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"],[3,"value"],["align","start"]],template:function(e,i){e&1&&(g(0,"3"),f(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),g(6,"Agregar Producto"),p()(),f(7,"mat-card-content")(8,"form",1),F("ngSubmit",function(){return i.agregarDetallePedido()}),f(9,"mat-grid-list",2),Ze(10,R5,9,4,"mat-card-title",null,Qe),f(12,"mat-card-title")(13,"button",3),g(14,"Agregar"),p()()()()()()()()),e&2&&(h(8),x("formGroup",i.agregarCarritoForm),h(2),Ke(i.formConfigs()))},dependencies:[Si,ut,rn,er,nr,tr,bn,on,jn,ir,kr,yn,Ei,Ci,Ln,Vn,bi,ii,no,Yt,ni,io],styles:[sC]})};var tu=class t{localStorageService=d(an);dataService=d(Je);constructor(){}getdetalleCompras(){var n=[],e=this.localStorageService.getItem(j.DETALLE_PEDIDOS_COMPRA_ID);return e&&(n=e),n}eliminarDetallesPedidoCompra(){this.localStorageService.removeItem(j.DETALLE_PEDIDOS_COMPRA_ID)}registrarCompra(){var n=this.getdetalleCompras();for(let e of n)this.registrarCompraInventario(e);this.eliminarDetallesPedidoCompra()}registrarCompraInventario(n){let e=j.loteInventarioVacio();e.cantidad_actual=n.cantidad,e.cantidad_inicial=n.cantidad,e.costo_unitario=n.precio_unitario_venta,e.fecha_ingreso=new Date,e.producto=n.producto,e=this.dataService.pushLoteInventario(e);let i=this.dataService.getTiposMovimiento(),r=j.movimientoInventarioVacio();r.cantidad=n.cantidad,r.fecha=new Date,r.producto=n.producto,r.tipo_movimiento=i[i.findIndex(o=>o.id==1)],r.lote=e,this.dataService.pushMovimientoInventario(r),n.producto.stock_web&&n.producto.stock_web>0?n.producto.stock_web+=n.cantidad_web:n.producto.stock_web=n.cantidad_web,n.producto.stock_local&&n.producto.stock_local>0?n.producto.stock_local+=n.cantidad_local:n.producto.stock_local=n.cantidad_local,this.dataService.editarProducto(n.producto)}aumentarDetalleCompra(n){var e=this.getdetalleCompras();for(let i of e)i.id==n.id&&(i.cantidad++,i.cantidad_web++,this.editarDetalleCompra(i));return e}disminuirDetalleCompra(n){var e=this.getdetalleCompras();for(let i of e)i.id==n.id&&i.cantidad>0&&(i.cantidad--,i.cantidad_web--,this.editarDetalleCompra(i));return e}editarDetalleCompra(n){var e=this.getdetalleCompras(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(j.DETALLE_PEDIDOS_COMPRA_ID,i)}pushDetalleCompra(n){var e=this.getdetalleCompras();e.length>0?n.id=e[e.length-1].id+1:n.id=1,e.push(n),this.localStorageService.setItem(j.DETALLE_PEDIDOS_COMPRA_ID,e)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var pk=()=>["producto","cantidad","cantidad_web","cantidad_local","precio","opciones"];function k5(t,n){t&1&&(f(0,"th",16),g(1,"Producto"),p()),t&2&&E("text-align","center")}function O5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),Dr(" ",e.producto.sku," ",e.producto.nombre," ")}}function N5(t,n){t&1&&(f(0,"th",16),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function P5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad)}}function F5(t,n){t&1&&(f(0,"th",16),g(1,"Cantidad Stock Web"),p()),t&2&&E("text-align","center")}function L5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad_web)}}function V5(t,n){t&1&&(f(0,"th",16),g(1,"Cantidad Stock Local"),p()),t&2&&E("text-align","center")}function j5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad_local)}}function B5(t,n){t&1&&(f(0,"th",16),g(1,"Precio"),p()),t&2&&E("text-align","center")}function H5(t,n){if(t&1&&(f(0,"td",17),g(1),Wt(2,"currency"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",mi(2,3,e.precio_unitario_venta))}}function U5(t,n){t&1&&(f(0,"th",18),g(1,"OPCIONES"),p())}function z5(t,n){if(t&1){let e=ze();f(0,"td",17)(1,"button",19),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.aumentarDetalleCompra(r))}),f(2,"mat-icon"),g(3,"arrow_circle_up"),p()(),f(4,"button",19),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.disminuirDetalleCompra(r))}),f(5,"mat-icon"),g(6,"arrow_circle_down"),p()()()}}function $5(t,n){t&1&&U(0,"tr",20)}function G5(t,n){t&1&&U(0,"tr",21)}function W5(t,n){if(t&1&&U(0,"textarea",4),t&2){let e=y().$implicit,i=y();x("formControlName",e.name)("errorStateMatcher",i.matcher)}}function q5(t,n){if(t&1&&(f(0,"mat-option",8),g(1),p()),t&2){let e=n.$implicit;x("value",e.value),h(),ve(e.name)}}function Y5(t,n){if(t&1&&(f(0,"mat-select",5),Ze(1,q5,2,2,"mat-option",8,Qe),p()),t&2){let e=y().$implicit,i=y();x("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ke(e.options)}}function Q5(t,n){if(t&1){let e=ze();f(0,"input",9),F("change",function(r){Ae(e);let o=y(2);return Re(o.inputChange(r))}),p()}if(t&2){let e=y().$implicit,i=y();x("maxlength",Xn(e.max&&e.max>0&&e.type=="text"?e.max:1e3))("max",Xn(e.max&&e.max>0&&e.type=="number"?e.max:1e6))("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Z5(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Seleccione"," "+e.label)}}function K5(t,n){if(t&1&&(f(0,"mat-hint",10),g(1),p()),t&2){let e=y(2).$implicit;h(),z("Ingrese"," "+e.label)}}function X5(t,n){if(t&1&&(I(0,Z5,2,1,"mat-hint",10),I(1,K5,2,1,"mat-hint",10)),t&2){let e,i,r=y().$implicit,o=y();T((e=o.getFormControl(r.name))!=null&&e.errors&&r.controlType==="select"?0:-1),h(),T((i=o.getFormControl(r.name))!=null&&i.errors&&r.controlType!="select"?1:-1)}}function J5(t,n){if(t&1&&(f(0,"mat-hint",7),g(1),p()),t&2){let e,i=y().$implicit,r=y();h(),wr("",i.min&&i.min>0?i.min:"-","/",i.type=="number"?(e=r.getFormControl(i.name))==null?null:e.value:(e=r.getFormControl(i.name))==null?null:e.value.length,"/",i.max&&i.max>0?i.max:"-")}}function e8(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),I(4,W5,1,2,"textarea",4)(5,Y5,3,3,"mat-select",5)(6,Q5,1,7,"input",6),I(7,X5,2,2),I(8,J5,2,3,"mat-hint",7),p()()),t&2){let e,i,r=n.$implicit,o=y();h(3),ve(r.label),h(),T((e=r.controlType)==="textarea"?4:e==="select"?5:6),h(3),T((i=o.getFormControl(r.name))!=null&&i.errors?7:-1),h(),T(r.controlType=="input"&&(r.max||r.min)?8:-1)}}var Ah=class t{innerWidths="0";document=d(V);dialog=d(Ki);compraService=d(tu);detalleCompraList=S([]);constructor(){}ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarListas()}cargarListas(){this.detalleCompraList.update(n=>[...this.compraService.getdetalleCompras()])}registrarCompra(){this.compraService.registrarCompra(),this.cargarListas()}aumentarDetalleCompra(n){this.detalleCompraList.update(e=>[...this.compraService.aumentarDetalleCompra(n)])}disminuirDetalleCompra(n){this.detalleCompraList.update(e=>[...this.compraService.disminuirDetalleCompra(n)])}agregarDetalleCompra(){let n=ge.getMatDialogConf();this.dialog.open(lC,n).afterClosed().subscribe(i=>{this.cargarListas()})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-compras"]],decls:33,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","cantidad_web"],["matColumnDef","cantidad_local"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Compra de productos "),p()(),f(5,"table",2),ee(6,3),N(7,k5,2,2,"th",4)(8,O5,2,4,"td",5),te(),ee(9,6),N(10,N5,2,2,"th",4)(11,P5,2,3,"td",5),te(),ee(12,7),N(13,F5,2,2,"th",4)(14,L5,2,3,"td",5),te(),ee(15,8),N(16,V5,2,2,"th",4)(17,j5,2,3,"td",5),te(),ee(18,9),N(19,B5,2,2,"th",4)(20,H5,3,5,"td",5),te(),ee(21,10),N(22,U5,2,0,"th",11)(23,z5,7,0,"td",12),te(),N(24,$5,1,0,"tr",13)(25,G5,1,0,"tr",14),p()(),f(26,"mat-expansion-panel",1)(27,"mat-expansion-panel-header")(28,"mat-panel-description")(29,"button",15),F("click",function(){return i.registrarCompra()}),g(30,"Registrar Compra"),p(),f(31,"button",15),F("click",function(){return i.agregarDetalleCompra()}),g(32,"Agregar Producto"),p()()()()()),e&2&&(h(),E("overflow","auto"),h(4),x("dataSource",i.detalleCompraList()),h(19),x("matHeaderRowDef",ot(5,pk)),h(),x("matRowDefColumns",ot(6,pk)))},dependencies:[Mi,Rr,ri,oi,xi,Ar,ut,ro,Bn,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,Er],encapsulation:2})},lC=class t{innerWidths="0";document=d(V);_snackBar=d(Qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);dataService=d(Je);compraService=d(tu);agregarCompraForm;productoList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas();var n=this.formService.newAgregarDetalleCompraControls(this.productoList());this.agregarCompraForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}inputChange(n){let e=this.getFormControl("cantidad"),i=this.getFormControl("cantidad_web"),r=this.getFormControl("cantidad_local"),o=e?.value|0,a=i?.value|0,s=r?.value|0;if(o>0){let l=0,c=o,u=0,m=o;a>0&&(m=o-a),s>0&&(c=o-s),o-(a+s)>0&&(l=o-(a+s),u=l),i?.setValidators([re.required,re.min(l),re.max(c)]),r?.setValidators([re.required,re.min(u),re.max(m)])}}getFormControl(n){return this.agregarCompraForm.get(n)}compareIds(n,e){return n.id==e.id}cargarListas(){this.productoList.update(n=>[...this.dataService.getProductos()])}agregarDetalleCompra(){if(this.validarDatos()){let n=this.agregarCompraForm.value;if(parseInt(n.cantidad)-(parseInt(n.cantidad_local)+parseInt(n.cantidad_web))>0){ge.openSnackBar("Cantidades no cuadran","ok",this._snackBar);return}let e=j.detalleCompraVacio();e.cantidad=parseInt(n.cantidad),e.cantidad_local=parseInt(n.cantidad_local),e.cantidad_web=parseInt(n.cantidad_web),e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.compraService.pushDetalleCompra(e),ge.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else ge.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCompraForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","maxlength","max","errorStateMatcher"],["align","end"],[3,"value"],["matInput","","required","",3,"change","type","formControlName","maxlength","max","errorStateMatcher"],["align","start"]],template:function(e,i){e&1&&(g(0,"3"),f(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),g(6,"Agregar Producto"),p()(),f(7,"mat-card-content")(8,"form",1),F("ngSubmit",function(){return i.agregarDetalleCompra()}),f(9,"mat-grid-list",2),Ze(10,e8,9,4,"mat-card-title",null,Qe),f(12,"mat-card-title")(13,"button",3),g(14,"Agregar"),p()()()()()()()()),e&2&&(h(8),x("formGroup",i.agregarCompraForm),h(2),Ke(i.formConfigs()))},dependencies:[Si,ut,rn,er,nr,tr,bn,on,jn,ir,kr,yn,Ei,Ci,Ln,Vn,bi,ii,no,Yt,ni,io],encapsulation:2})};var nu=class t{dataService=d(Je);_snackBar=d(Qt);constructor(){}getMovimientosProducto(n){let e=this.dataService.getMovimientosInventario(),i=[];for(let r of e)r.producto.id==n.id&&i.push(r);return i}getMovimientosTipoMovimiento(n){let e=this.dataService.getMovimientosInventario(),i=[];for(let r of e)r.tipo_movimiento.id==n.id&&i.push(r);return i}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var hk=()=>["sku","nombre","descripcion","stock_web","stock_local","estado","opciones"];function t8(t,n){t&1&&(f(0,"th",17),g(1,"Codigo"),p()),t&2&&E("text-align","center")}function n8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.sku," ")}}function i8(t,n){t&1&&(f(0,"th",17),g(1,"Nombre"),p()),t&2&&E("text-align","center")}function r8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.nombre)}}function o8(t,n){t&1&&(f(0,"th",17),g(1,"Descripcion"),p()),t&2&&E("text-align","center")}function a8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.descripcion)}}function s8(t,n){t&1&&(f(0,"th",17),g(1,"Stock Web"),p()),t&2&&E("text-align","center")}function l8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.stock_web," ")}}function c8(t,n){t&1&&(f(0,"th",17),g(1,"Stock Local"),p()),t&2&&E("text-align","center")}function d8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.stock_local," ")}}function u8(t,n){t&1&&(f(0,"th",17),g(1,"Periodo"),p()),t&2&&E("text-align","center")}function f8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.estado.descripcion," ")}}function m8(t,n){t&1&&(f(0,"th",19),g(1,"Ver Movimientos"),p())}function p8(t,n){if(t&1){let e=ze();f(0,"td",18)(1,"button",20),F("click",function(){let r=Ae(e).$implicit,o=y();return Re(o.verMovimientosProducto(r))}),f(2,"mat-icon"),g(3,"article"),p()()()}}function h8(t,n){t&1&&U(0,"tr",21)}function g8(t,n){t&1&&U(0,"tr",22)}var Rh=()=>["sku","producto","fecha","lote","tipo_movimiento","cantidad"];function v8(t,n){t&1&&(f(0,"th",12),g(1,"Codigo"),p()),t&2&&E("text-align","center")}function _8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.sku," ")}}function y8(t,n){t&1&&(f(0,"th",12),g(1,"Producto"),p()),t&2&&E("text-align","center")}function b8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.nombre," ")}}function C8(t,n){t&1&&(f(0,"th",12),g(1,"Fecha"),p()),t&2&&E("text-align","center")}function D8(t,n){if(t&1&&(f(0,"td",13),g(1),Wt(2,"date"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",js(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function w8(t,n){t&1&&(f(0,"th",12),g(1,"Lote"),p()),t&2&&E("text-align","center")}function x8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.lote.id)}}function E8(t,n){t&1&&(f(0,"th",12),g(1,"Movimiento"),p()),t&2&&E("text-align","center")}function S8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.tipo_movimiento.descripcion)}}function M8(t,n){t&1&&(f(0,"th",12),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function I8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad," ")}}function T8(t,n){t&1&&U(0,"tr",14)}function A8(t,n){t&1&&U(0,"tr",15)}function R8(t,n){t&1&&(f(0,"th",12),g(1,"Codigo"),p()),t&2&&E("text-align","center")}function k8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.sku," ")}}function O8(t,n){t&1&&(f(0,"th",12),g(1,"Producto"),p()),t&2&&E("text-align","center")}function N8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.nombre," ")}}function P8(t,n){t&1&&(f(0,"th",12),g(1,"Fecha"),p()),t&2&&E("text-align","center")}function F8(t,n){if(t&1&&(f(0,"td",13),g(1),Wt(2,"date"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",js(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function L8(t,n){t&1&&(f(0,"th",12),g(1,"Lote"),p()),t&2&&E("text-align","center")}function V8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.lote.id)}}function j8(t,n){t&1&&(f(0,"th",12),g(1,"Movimiento"),p()),t&2&&E("text-align","center")}function B8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.tipo_movimiento.descripcion)}}function H8(t,n){t&1&&(f(0,"th",12),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function U8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad," ")}}function z8(t,n){t&1&&U(0,"tr",14)}function $8(t,n){t&1&&U(0,"tr",15)}var gk=()=>["cuenta_movimiento","tipo_movimiento","fecha","sku","producto","lote","cantidad"];function G8(t,n){t&1&&(f(0,"th",13),g(1,"Cuenta Movimiento"),p()),t&2&&E("text-align","center")}function W8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.tipo_movimiento.cuenta)}}function q8(t,n){t&1&&(f(0,"th",13),g(1,"Movimiento"),p()),t&2&&E("text-align","center")}function Y8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.tipo_movimiento.descripcion)}}function Q8(t,n){t&1&&(f(0,"th",13),g(1,"Codigo"),p()),t&2&&E("text-align","center")}function Z8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.sku," ")}}function K8(t,n){t&1&&(f(0,"th",13),g(1,"Producto"),p()),t&2&&E("text-align","center")}function X8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.nombre," ")}}function J8(t,n){t&1&&(f(0,"th",13),g(1,"Fecha"),p()),t&2&&E("text-align","center")}function eG(t,n){if(t&1&&(f(0,"td",14),g(1),Wt(2,"date"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",js(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function tG(t,n){t&1&&(f(0,"th",13),g(1,"Lote"),p()),t&2&&E("text-align","center")}function nG(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.lote.id)}}function iG(t,n){t&1&&(f(0,"th",13),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function rG(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad," ")}}function oG(t,n){t&1&&U(0,"tr",15)}function aG(t,n){t&1&&U(0,"tr",16)}var vk=()=>["fecha","sku","producto","lote","cantidad"];function sG(t,n){t&1&&(f(0,"th",11),g(1,"Codigo"),p()),t&2&&E("text-align","center")}function lG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.sku," ")}}function cG(t,n){t&1&&(f(0,"th",11),g(1,"Producto"),p()),t&2&&E("text-align","center")}function dG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.producto.nombre," ")}}function uG(t,n){t&1&&(f(0,"th",11),g(1,"Fecha"),p()),t&2&&E("text-align","center")}function fG(t,n){if(t&1&&(f(0,"td",12),g(1),Wt(2,"date"),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",js(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function mG(t,n){t&1&&(f(0,"th",11),g(1,"Lote"),p()),t&2&&E("text-align","center")}function pG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.lote.id)}}function hG(t,n){t&1&&(f(0,"th",11),g(1,"Cantidad"),p()),t&2&&E("text-align","center")}function gG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cantidad," ")}}function vG(t,n){t&1&&U(0,"tr",13)}function _G(t,n){t&1&&U(0,"tr",14)}function yG(t,n){if(t&1&&(f(0,"div")(1,"p"),g(2),p(),f(3,"table",1),ee(4,2),N(5,sG,2,2,"th",3)(6,lG,2,3,"td",4),te(),ee(7,5),N(8,cG,2,2,"th",3)(9,dG,2,3,"td",4),te(),ee(10,6),N(11,uG,2,2,"th",3)(12,fG,3,6,"td",4),te(),ee(13,7),N(14,mG,2,2,"th",3)(15,pG,2,3,"td",4),te(),ee(16,8),N(17,hG,2,2,"th",3)(18,gG,2,3,"td",4),te(),N(19,vG,1,0,"tr",9)(20,_G,1,0,"tr",10),p()()),t&2){let e=n.$implicit,i=y();h(2),Dr("",e.cuenta,"-",e.descripcion),h(),x("dataSource",i.movimientosPorTipo(e)),h(16),x("matHeaderRowDef",ot(5,vk)),h(),x("matRowDefColumns",ot(6,vk))}}var _k=()=>["cuenta_movimiento","tipo_movimiento"];function bG(t,n){t&1&&(f(0,"th",8),g(1,"Cuenta"),p()),t&2&&E("text-align","center")}function CG(t,n){if(t&1&&(f(0,"td",9),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.cuenta)}}function DG(t,n){t&1&&(f(0,"th",8),g(1,"Movimiento"),p()),t&2&&E("text-align","center")}function wG(t,n){if(t&1&&(f(0,"td",9),g(1),p()),t&2){let e=n.$implicit;E("text-align","center"),h(),z(" ",e.descripcion)}}function xG(t,n){t&1&&U(0,"tr",10)}function EG(t,n){t&1&&U(0,"tr",11)}var kh=class t{constructor(n){this._router=n}innerWidths="0";document=d(V);dialog=d(Ki);dataService=d(Je);productosList=S([]);tipoMovimientoList=S([]);ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarListas()}cargarListas(){this.productosList.update(n=>this.dataService.getProductos()),this.tipoMovimientoList.update(n=>this.dataService.getTiposMovimiento())}verCompras(){this.verMovimientosTipo(this.tipoMovimientoList()[this.tipoMovimientoList().findIndex(n=>n.id==1)])}verVentas(){this.verMovimientosTipo(this.tipoMovimientoList()[this.tipoMovimientoList().findIndex(n=>n.id==2)])}verMovimientosProducto(n){let e=ge.getMatDialogConf();e.data={producto:n},this.dialog.open(cC,e)}verMovimientosTipo(n){let e=ge.getMatDialogConf();e.data={tipoMovimiento:n},this.dialog.open(dC,e)}verMovimientosDiario(){let n=ge.getMatDialogConf();this.dialog.open(uC,n)}verMovimientosMayor(){let n=ge.getMatDialogConf();this.dialog.open(fC,n)}verCatalogoCuentas(){let n=ge.getMatDialogConf();this.dialog.open(mC,n)}static \u0275fac=function(e){return new(e||t)(J(vn))};static \u0275cmp=O({type:t,selectors:[["app-inventarios"]],decls:42,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","nombre"],["matColumnDef","descripcion"],["matColumnDef","stock_web"],["matColumnDef","stock_local"],["matColumnDef","estado"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["aria-label","Ver Movimientos","matMiniFab","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Compra de productos "),p()(),f(5,"table",2),ee(6,3),N(7,t8,2,2,"th",4)(8,n8,2,3,"td",5),te(),ee(9,6),N(10,i8,2,2,"th",4)(11,r8,2,3,"td",5),te(),ee(12,7),N(13,o8,2,2,"th",4)(14,a8,2,3,"td",5),te(),ee(15,8),N(16,s8,2,2,"th",4)(17,l8,2,3,"td",5),te(),ee(18,9),N(19,c8,2,2,"th",4)(20,d8,2,3,"td",5),te(),ee(21,10),N(22,u8,2,2,"th",4)(23,f8,2,3,"td",5),te(),ee(24,11),N(25,m8,2,0,"th",12)(26,p8,4,0,"td",13),te(),N(27,h8,1,0,"tr",14)(28,g8,1,0,"tr",15),p()(),f(29,"mat-expansion-panel",1)(30,"mat-expansion-panel-header")(31,"mat-panel-description")(32,"button",16),F("click",function(){return i.verCompras()}),g(33,"Libro Compras"),p(),f(34,"button",16),F("click",function(){return i.verVentas()}),g(35,"Libro Ventas"),p(),f(36,"button",16),F("click",function(){return i.verMovimientosDiario()}),g(37,"Libro Diario"),p(),f(38,"button",16),F("click",function(){return i.verMovimientosMayor()}),g(39,"Libro Mayor"),p(),f(40,"button",16),F("click",function(){return i.verCatalogoCuentas()}),g(41,"Catalogo Cuentas"),p()()()()()),e&2&&(h(),E("overflow","auto"),h(4),x("dataSource",i.productosList()),h(22),x("matHeaderRowDef",ot(5,hk)),h(),x("matRowDefColumns",ot(6,hk)))},dependencies:[Mi,Rr,ri,oi,xi,Ar,ut,rh,Bn,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An],encapsulation:2})},cC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.producto.update(e=>n.producto)}innerWidths="0";document=d(V);dialogRef=d(Vt);inventarioService=d(nu);producto=S(j.productoVacio());movimientosList=S([]);ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.inventarioService.getMovimientosProducto(this.producto())])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-detalles"]],decls:26,vars:5,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","tipo_movimiento"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"table",0),ee(3,1),N(4,v8,2,2,"th",2)(5,_8,2,3,"td",3),te(),ee(6,4),N(7,y8,2,2,"th",2)(8,b8,2,3,"td",3),te(),ee(9,5),N(10,C8,2,2,"th",2)(11,D8,3,6,"td",3),te(),ee(12,6),N(13,w8,2,2,"th",2)(14,x8,2,3,"td",3),te(),ee(15,7),N(16,E8,2,2,"th",2)(17,S8,2,3,"td",3),te(),ee(18,8),N(19,M8,2,2,"th",2)(20,I8,2,3,"td",3),te(),N(21,T8,1,0,"tr",9)(22,A8,1,0,"tr",10),p()(),f(23,"div")(24,"button",11),F("click",function(){return i.cerrarDetalles()}),g(25,"Cerrar"),p()()()),e&2&&(h(2),x("dataSource",i.movimientosList()),h(19),x("matHeaderRowDef",ot(3,Rh)),h(),x("matRowDefColumns",ot(4,Rh)))},dependencies:[Xo,ut,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,rn,$s],encapsulation:2})},dC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.tipoMovimiento.update(e=>n.tipoMovimiento)}innerWidths="0";document=d(V);dialogRef=d(Vt);inventarioService=d(nu);tipoMovimiento=S(j.tipoMovimientoVacio());movimientosList=S([]);ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.inventarioService.getMovimientosTipoMovimiento(this.tipoMovimiento())])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)(J(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:28,vars:6,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","tipo_movimiento"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3),p(),f(4,"table",0),ee(5,1),N(6,R8,2,2,"th",2)(7,k8,2,3,"td",3),te(),ee(8,4),N(9,O8,2,2,"th",2)(10,N8,2,3,"td",3),te(),ee(11,5),N(12,P8,2,2,"th",2)(13,F8,3,6,"td",3),te(),ee(14,6),N(15,L8,2,2,"th",2)(16,V8,2,3,"td",3),te(),ee(17,7),N(18,j8,2,2,"th",2)(19,B8,2,3,"td",3),te(),ee(20,8),N(21,H8,2,2,"th",2)(22,U8,2,3,"td",3),te(),N(23,z8,1,0,"tr",9)(24,$8,1,0,"tr",10),p()(),f(25,"div")(26,"button",11),F("click",function(){return i.cerrarDetalles()}),g(27,"Cerrar"),p()()()),e&2&&(h(3),z("Movimientos de ",i.tipoMovimiento().descripcion),h(),x("dataSource",i.movimientosList()),h(19),x("matHeaderRowDef",ot(4,Rh)),h(),x("matRowDefColumns",ot(5,Rh)))},dependencies:[Xo,ut,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,rn,$s],encapsulation:2})},uC=class t{innerWidths="0";document=d(V);dialogRef=d(Vt);dataService=d(Je);movimientosList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.dataService.getMovimientosInventario()])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:31,vars:5,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","cuenta_movimiento"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","tipo_movimiento"],["matColumnDef","sku"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3,"Movimientos de Libro Diario"),p(),f(4,"table",0),ee(5,1),N(6,G8,2,2,"th",2)(7,W8,2,3,"td",3),te(),ee(8,4),N(9,q8,2,2,"th",2)(10,Y8,2,3,"td",3),te(),ee(11,5),N(12,Q8,2,2,"th",2)(13,Z8,2,3,"td",3),te(),ee(14,6),N(15,K8,2,2,"th",2)(16,X8,2,3,"td",3),te(),ee(17,7),N(18,J8,2,2,"th",2)(19,eG,3,6,"td",3),te(),ee(20,8),N(21,tG,2,2,"th",2)(22,nG,2,3,"td",3),te(),ee(23,9),N(24,iG,2,2,"th",2)(25,rG,2,3,"td",3),te(),N(26,oG,1,0,"tr",10)(27,aG,1,0,"tr",11),p()(),f(28,"div")(29,"button",12),F("click",function(){return i.cerrarDetalles()}),g(30,"Cerrar"),p()()()),e&2&&(h(4),x("dataSource",i.movimientosList()),h(22),x("matHeaderRowDef",ot(3,gk)),h(),x("matRowDefColumns",ot(4,gk)))},dependencies:[Xo,ut,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,rn,$s],encapsulation:2})},fC=class t{innerWidths="0";document=d(V);dialogRef=d(Vt);dataService=d(Je);movimientosList=S([]);tipoMovimientoList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.dataService.getMovimientosInventario()]),this.tipoMovimientoList.update(n=>[...this.dataService.getTiposMovimiento()])}movimientosPorTipo(n){let e=this.movimientosList(),i=[];for(let r of e)r.tipo_movimiento.id==n.id&&i.push(r);return i}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:9,vars:0,consts:[["mat-stroked-button","","color","primary",3,"click"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3,"Movimientos de Libro Mayor"),p()(),Ze(4,yG,21,7,"div",null,Qe),f(6,"div")(7,"button",0),F("click",function(){return i.cerrarDetalles()}),g(8,"Cerrar"),p()()()),e&2&&(h(4),Ke(i.tipoMovimientoList()))},dependencies:[Xo,ut,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,rn,$s],encapsulation:2})},mC=class t{innerWidths="0";document=d(V);dialogRef=d(Vt);dataService=d(Je);tipoMovimientoList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.tipoMovimientoList.update(n=>[...this.dataService.getTiposMovimiento()])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:16,vars:5,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","cuenta_movimiento"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","tipo_movimiento"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3,"Catalogo de Cuentas"),p(),f(4,"table",0),ee(5,1),N(6,bG,2,2,"th",2)(7,CG,2,3,"td",3),te(),ee(8,4),N(9,DG,2,2,"th",2)(10,wG,2,3,"td",3),te(),N(11,xG,1,0,"tr",5)(12,EG,1,0,"tr",6),p()(),f(13,"div")(14,"button",7),F("click",function(){return i.cerrarDetalles()}),g(15,"Cerrar"),p()()()),e&2&&(h(4),x("dataSource",i.tipoMovimientoList()),h(7),x("matHeaderRowDef",ot(3,_k)),h(),x("matRowDefColumns",ot(4,_k)))},dependencies:[Xo,ut,Cn,wn,Mn,xn,Dn,In,En,Sn,Tn,An,rn],encapsulation:2})};var yk=[{path:"",redirectTo:"/menu",pathMatch:"full"},{path:"menu",component:Sh,children:[{path:"",redirectTo:"/menu/login",pathMatch:"full"},{path:"login",component:Mh},{path:"productos",component:Ih},{path:"carrito",component:Eh},{path:"ventas",component:Th},{path:"compras",component:Ah},{path:"inventario",component:kh}]}];var SG=[Je,Hn,an,Un,Rl,Ii],bk={providers:[SG,{provide:Xb,useValue:{hasBackdrop:!1}},iv(),Tb(yk,Ab()),TI(II())]};var Oh=class t{title=S("techstoresv");static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&U(0,"router-outlet")},dependencies:[ja],styles:['[_nghost-%COMP%]{--bright-blue: oklch(51.01% .274 263.83);--electric-violet: oklch(53.18% .28 296.97);--french-violet: oklch(47.66% .246 305.88);--vivid-pink: oklch(69.02% .277 332.77);--hot-red: oklch(61.42% .238 15.34);--orange-red: oklch(63.32% .24 31.68);--gray-900: oklch(19.37% .006 300.98);--gray-700: oklch(36.98% .014 302.71);--gray-400: oklch(70.9% .015 304.04);--red-to-pink-to-purple-vertical-gradient: linear-gradient( 180deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--red-to-pink-to-purple-horizontal-gradient: linear-gradient( 90deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--pill-accent: var(--bright-blue);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol;box-sizing:border-box;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;display:block;height:100dvh}h1[_ngcontent-%COMP%]{font-size:3.125rem;color:var(--gray-900);font-weight:500;line-height:100%;letter-spacing:-.125rem;margin:0;font-family:Inter Tight,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol}p[_ngcontent-%COMP%]{margin:0;color:var(--gray-700)}main[_ngcontent-%COMP%]{width:100%;min-height:100%;display:flex;justify-content:center;align-items:center;padding:1rem;box-sizing:inherit;position:relative}.angular-logo[_ngcontent-%COMP%]{max-width:9.2rem}.content[_ngcontent-%COMP%]{display:flex;justify-content:space-around;width:100%;max-width:700px;margin-bottom:3rem}.content[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin-top:1.75rem}.content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin-top:1.5rem}.divider[_ngcontent-%COMP%]{width:1px;background:var(--red-to-pink-to-purple-vertical-gradient);margin-inline:.5rem}.pill-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:start;flex-wrap:wrap;gap:1.25rem}.pill[_ngcontent-%COMP%]{display:flex;align-items:center;--pill-accent: var(--bright-blue);background:color-mix(in srgb,var(--pill-accent) 5%,transparent);color:var(--pill-accent);padding-inline:.75rem;padding-block:.375rem;border-radius:2.75rem;border:0;transition:background .3s ease;font-family:var(--inter-font);font-size:.875rem;font-style:normal;font-weight:500;line-height:1.4rem;letter-spacing:-.00875rem;text-decoration:none;white-space:nowrap}.pill[_ngcontent-%COMP%]:hover{background:color-mix(in srgb,var(--pill-accent) 15%,transparent)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+1){--pill-accent: var(--bright-blue)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+2){--pill-accent: var(--electric-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+3){--pill-accent: var(--french-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+4), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+5), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+6){--pill-accent: var(--hot-red)}.pill-group[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{margin-inline-start:.25rem}.social-links[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.73rem;margin-top:1.5rem}.social-links[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{transition:fill .3s ease;fill:var(--gray-400)}.social-links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover   svg[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{fill:var(--gray-900)}@media screen and (max-width:650px){.content[_ngcontent-%COMP%]{flex-direction:column;width:max-content}.divider[_ngcontent-%COMP%]{height:1px;width:100%;background:var(--red-to-pink-to-purple-horizontal-gradient);margin-block:1.5rem}}']})};Wy(Oh,bk).catch(t=>console.error(t));
