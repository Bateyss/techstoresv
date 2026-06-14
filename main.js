var hk=Object.defineProperty,gk=Object.defineProperties;var vk=Object.getOwnPropertyDescriptors;var Jd=Object.getOwnPropertySymbols;var dC=Object.prototype.hasOwnProperty,uC=Object.prototype.propertyIsEnumerable;var cC=(t,n,e)=>n in t?hk(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,D=(t,n)=>{for(var e in n||={})dC.call(n,e)&&cC(t,e,n[e]);if(Jd)for(var e of Jd(n))uC.call(n,e)&&cC(t,e,n[e]);return t},fe=(t,n)=>gk(t,vk(n));var fC=(t,n)=>{var e={};for(var i in t)dC.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&Jd)for(var i of Jd(t))n.indexOf(i)<0&&uC.call(t,i)&&(e[i]=t[i]);return e};var Qt=null,eu=!1,Rh=1,_k=null,bt=Symbol("SIGNAL");function ce(t){let n=Qt;return Qt=t,n}function tu(){return Qt}var oo={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ao(t){if(eu)throw new Error("");if(Qt===null)return;Qt.consumerOnSignalRead(t);let n=Qt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Qt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Qt.producers,e!==void 0&&e.producer===t)){Qt.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Qt&&(!i||bk(r,Qt)))return;let o=Ka(Qt),a={producer:t,consumer:Qt,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};Qt.producersTail=a,n!==void 0?n.nextProducer=a:Qt.producers=a,o&&gC(t,a)}function mC(){Rh++}function Ko(t){if(!(Ka(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Rh)){if(!t.producerMustRecompute(t)&&!Za(t)){Qa(t);return}t.producerRecomputeValue(t),Qa(t)}}function kh(t){if(t.consumers===void 0)return;let n=eu;eu=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||yk(i)}}finally{eu=n}}function Oh(){return Qt?.consumerAllowSignalWrites!==!1}function yk(t){t.dirty=!0,kh(t),t.consumerMarkedDirty?.(t)}function Qa(t){t.dirty=!1,t.lastCleanEpoch=Rh}function kr(t){return t&&pC(t),ce(t)}function pC(t){t.producersTail=void 0,t.recomputing=!0}function so(t,n){ce(n),t&&hC(t)}function hC(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ka(t))do e=Nh(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Za(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Ko(e),i!==e.version))return!0}return!1}function lo(t){if(Ka(t)){let n=t.producers;for(;n!==void 0;)n=Nh(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function gC(t,n){let e=t.consumersTail,i=Ka(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)gC(r.producer,r)}function Nh(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ka(n)){let o=n.producers;for(;o!==void 0;)o=Nh(o)}return e}function Ka(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Tl(t){_k?.(t)}function bk(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Al(t,n){return Object.is(t,n)}function Rl(t,n){let e=Object.create(Ck);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Ko(e),ao(e),e.value===rr)throw e.error;return e.value};return i[bt]=e,Tl(e),i}var Qo=Symbol("UNSET"),Zo=Symbol("COMPUTING"),rr=Symbol("ERRORED"),Ck=fe(D({},oo),{value:Qo,dirty:!0,error:null,equal:Al,kind:"computed",producerMustRecompute(t){return t.value===Qo||t.value===Zo},producerRecomputeValue(t){if(t.value===Zo)throw new Error("");let n=t.value;t.value=Zo;let e=kr(t),i,r=!1;try{i=t.computation(),ce(null),r=n!==Qo&&n!==rr&&i!==rr&&t.equal(n,i)}catch(o){i=rr,t.error=o}finally{so(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Dk(){throw new Error}var vC=Dk;function _C(t){vC(t)}function Ph(t){vC=t}var wk=null;function Fh(t,n){let e=Object.create(kl);e.value=t,n!==void 0&&(e.equal=n);let i=()=>yC(e);return i[bt]=e,Tl(e),[i,a=>Xo(e,a),a=>nu(e,a)]}function yC(t){return ao(t),t.value}function Xo(t,n){Oh()||_C(t),t.equal(t.value,n)||(t.value=n,Ek(t))}function nu(t,n){Oh()||_C(t),Xo(t,n(t.value))}var kl=fe(D({},oo),{equal:Al,value:void 0,kind:"signal"});function Ek(t){t.version++,mC(),kh(t),wk?.(t)}var Lh=fe(D({},oo),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Vh(t){if(t.dirty=!1,t.version>0&&!Za(t))return;t.version++;let n=kr(t);try{t.cleanup(),t.fn()}finally{so(t,n)}}function xe(t){return typeof t=="function"}function Xa(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var iu=Xa(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Jo(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var pe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(xe(i))try{i()}catch(o){n=o instanceof iu?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{bC(o)}catch(a){n=n??[],a instanceof iu?n=[...n,...a.errors]:n.push(a)}}if(n)throw new iu(n)}}add(n){var e;if(n&&n!==this)if(this.closed)bC(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Jo(e,n)}remove(n){let{_finalizers:e}=this;e&&Jo(e,n),n instanceof t&&n._removeParent(this)}};pe.EMPTY=(()=>{let t=new pe;return t.closed=!0,t})();var jh=pe.EMPTY;function ru(t){return t instanceof pe||t&&"closed"in t&&xe(t.remove)&&xe(t.add)&&xe(t.unsubscribe)}function bC(t){xe(t)?t():t.unsubscribe()}var Ti={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Ja={setTimeout(t,n,...e){let{delegate:i}=Ja;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Ja;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function ou(t){Ja.setTimeout(()=>{let{onUnhandledError:n}=Ti;if(n)n(t);else throw t})}function Ol(){}var CC=Bh("C",void 0,void 0);function DC(t){return Bh("E",void 0,t)}function wC(t){return Bh("N",t,void 0)}function Bh(t,n,e){return{kind:t,value:n,error:e}}var ea=null;function es(t){if(Ti.useDeprecatedSynchronousErrorHandling){let n=!ea;if(n&&(ea={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=ea;if(ea=null,e)throw i}}else t()}function EC(t){Ti.useDeprecatedSynchronousErrorHandling&&ea&&(ea.errorThrown=!0,ea.error=t)}var ta=class extends pe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,ru(n)&&n.add(this)):this.destination=Mk}static create(n,e,i){return new Or(n,e,i)}next(n){this.isStopped?Uh(wC(n),this):this._next(n)}error(n){this.isStopped?Uh(DC(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Uh(CC,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},xk=Function.prototype.bind;function Hh(t,n){return xk.call(t,n)}var zh=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){au(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){au(i)}else au(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){au(e)}}},Or=class extends ta{constructor(n,e,i){super();let r;if(xe(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Ti.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Hh(n.next,o),error:n.error&&Hh(n.error,o),complete:n.complete&&Hh(n.complete,o)}):r=n}this.destination=new zh(r)}};function au(t){Ti.useDeprecatedSynchronousErrorHandling?EC(t):ou(t)}function Sk(t){throw t}function Uh(t,n){let{onStoppedNotification:e}=Ti;e&&Ja.setTimeout(()=>e(t,n))}var Mk={closed:!0,next:Ol,error:Sk,complete:Ol};var ts=typeof Symbol=="function"&&Symbol.observable||"@@observable";function $n(t){return t}function $h(...t){return Gh(t)}function Gh(t){return t.length===0?$n:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ye=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=Tk(e)?e:new Or(e,i,r);return es(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=xC(i),new i((r,o)=>{let a=new Or({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[ts](){return this}pipe(...e){return Gh(e)(this)}toPromise(e){return e=xC(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function xC(t){var n;return(n=t??Ti.Promise)!==null&&n!==void 0?n:Promise}function Ik(t){return t&&xe(t.next)&&xe(t.error)&&xe(t.complete)}function Tk(t){return t&&t instanceof ta||Ik(t)&&ru(t)}function Wh(t){return xe(t?.lift)}function De(t){return n=>{if(Wh(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function be(t,n,e,i,r){return new qh(t,n,e,i,r)}var qh=class extends ta{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};function SC(){return De((t,n)=>{let e=null;t._refCount++;let i=be(n,void 0,void 0,void 0,()=>{if(!t||t._refCount<=0||0<--t._refCount){e=null;return}let r=t._connection,o=e;e=null,r&&(!o||r===o)&&r.unsubscribe(),n.unsubscribe()});t.subscribe(i),i.closed||(e=t.connect())})}var Nl=class extends ye{constructor(n,e){super(),this.source=n,this.subjectFactory=e,this._subject=null,this._refCount=0,this._connection=null,Wh(n)&&(this.lift=n.lift)}_subscribe(n){return this.getSubject().subscribe(n)}getSubject(){let n=this._subject;return(!n||n.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:n}=this;this._subject=this._connection=null,n?.unsubscribe()}connect(){let n=this._connection;if(!n){n=this._connection=new pe;let e=this.getSubject();n.add(this.source.subscribe(be(e,void 0,()=>{this._teardown(),e.complete()},i=>{this._teardown(),e.error(i)},()=>this._teardown()))),n.closed&&(this._connection=null,n=pe.EMPTY)}return n}refCount(){return SC()(this)}};var ns={schedule(t){let n=requestAnimationFrame,e=cancelAnimationFrame,{delegate:i}=ns;i&&(n=i.requestAnimationFrame,e=i.cancelAnimationFrame);let r=n(o=>{e=void 0,t(o)});return new pe(()=>e?.(r))},requestAnimationFrame(...t){let{delegate:n}=ns;return(n?.requestAnimationFrame||requestAnimationFrame)(...t)},cancelAnimationFrame(...t){let{delegate:n}=ns;return(n?.cancelAnimationFrame||cancelAnimationFrame)(...t)},delegate:void 0};var MC=Xa(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var k=(()=>{class t extends ye{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new su(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new MC}next(e){es(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){es(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){es(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?jh:(this.currentObservers=null,o.push(e),new pe(()=>{this.currentObservers=null,Jo(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ye;return e.source=this,e}}return t.create=(n,e)=>new su(n,e),t})(),su=class extends k{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:jh}};var Ct=class extends k{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Pl={now(){return(Pl.delegate||Date).now()},delegate:void 0};var co=class extends k{constructor(n=1/0,e=1/0,i=Pl){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var lu=class extends pe{constructor(n,e){super()}schedule(n,e=0){return this}};var Fl={setInterval(t,n,...e){let{delegate:i}=Fl;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Fl;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var uo=class extends lu{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Fl.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Fl.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Jo(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Ak=1,Yh,Qh={};function IC(t){return t in Qh?(delete Qh[t],!0):!1}var TC={setImmediate(t){let n=Ak++;return Qh[n]=!0,Yh||(Yh=Promise.resolve()),Yh.then(()=>IC(n)&&t()),n},clearImmediate(t){IC(t)}};var{setImmediate:Rk,clearImmediate:kk}=TC,Ll={setImmediate(...t){let{delegate:n}=Ll;return(n?.setImmediate||Rk)(...t)},clearImmediate(t){let{delegate:n}=Ll;return(n?.clearImmediate||kk)(t)},delegate:void 0};var cu=class extends uo{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=Ll.setImmediate(n.flush.bind(n,void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(Ll.clearImmediate(e),n._scheduled===e&&(n._scheduled=void 0))}};var is=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};is.now=Pl.now;var fo=class extends is{constructor(n,e=is.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var du=class extends fo{flush(n){this._active=!0;let e=this._scheduled;this._scheduled=void 0;let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var uu=new du(cu);var Vl=new fo(uo),AC=Vl;var fu=class extends uo{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=ns.requestAnimationFrame(()=>n.flush(void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&e===n._scheduled&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(ns.cancelAnimationFrame(e),n._scheduled=void 0)}};var mu=class extends fo{flush(n){this._active=!0;let e;n?e=n.id:(e=this._scheduled,this._scheduled=void 0);let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var pu=new mu(fu);var ft=new ye(t=>t.complete());function hu(t){return t&&xe(t.schedule)}function Zh(t){return t[t.length-1]}function gu(t){return xe(Zh(t))?t.pop():void 0}function or(t){return hu(Zh(t))?t.pop():void 0}function RC(t,n){return typeof Zh(t)=="number"?t.pop():n}function OC(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(u){try{c(i.next(u))}catch(m){a(m)}}function l(u){try{c(i.throw(u))}catch(m){a(m)}}function c(u){u.done?o(u.value):r(u.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function kC(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function na(t){return this instanceof na?(this.v=t,this):new na(t)}function NC(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(_){return function(y){return Promise.resolve(y).then(_,m)}}function s(_,y){i[_]&&(r[_]=function(I){return new Promise(function(R,V){o.push([_,I,R,V])>1||l(_,I)})},y&&(r[_]=y(r[_])))}function l(_,y){try{c(i[_](y))}catch(I){v(o[0][3],I)}}function c(_){_.value instanceof na?Promise.resolve(_.value.v).then(u,m):v(o[0][2],_)}function u(_){l("next",_)}function m(_){l("throw",_)}function v(_,y){_(y),o.shift(),o.length&&l(o[0][0],o[0][1])}}function PC(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof kC=="function"?kC(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var vu=t=>t&&typeof t.length=="number"&&typeof t!="function";function _u(t){return xe(t?.then)}function yu(t){return xe(t[ts])}function bu(t){return Symbol.asyncIterator&&xe(t?.[Symbol.asyncIterator])}function Cu(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Ok(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Du=Ok();function wu(t){return xe(t?.[Du])}function Eu(t){return NC(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield na(e.read());if(r)return yield na(void 0);yield yield na(i)}}finally{e.releaseLock()}})}function xu(t){return xe(t?.getReader)}function et(t){if(t instanceof ye)return t;if(t!=null){if(yu(t))return Nk(t);if(vu(t))return Pk(t);if(_u(t))return Fk(t);if(bu(t))return FC(t);if(wu(t))return Lk(t);if(xu(t))return Vk(t)}throw Cu(t)}function Nk(t){return new ye(n=>{let e=t[ts]();if(xe(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Pk(t){return new ye(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Fk(t){return new ye(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,ou)})}function Lk(t){return new ye(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function FC(t){return new ye(n=>{jk(t,n).catch(e=>n.error(e))})}function Vk(t){return FC(Eu(t))}function jk(t,n){var e,i,r,o;return OC(this,void 0,void 0,function*(){try{for(e=PC(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function An(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Su(t,n=0){return De((e,i)=>{e.subscribe(be(i,r=>An(i,t,()=>i.next(r),n),()=>An(i,t,()=>i.complete(),n),r=>An(i,t,()=>i.error(r),n)))})}function Mu(t,n=0){return De((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function LC(t,n){return et(t).pipe(Mu(n),Su(n))}function VC(t,n){return et(t).pipe(Mu(n),Su(n))}function jC(t,n){return new ye(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function BC(t,n){return new ye(e=>{let i;return An(e,n,()=>{i=t[Du](),An(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>xe(i?.return)&&i.return()})}function Iu(t,n){if(!t)throw new Error("Iterable cannot be null");return new ye(e=>{An(e,n,()=>{let i=t[Symbol.asyncIterator]();An(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function HC(t,n){return Iu(Eu(t),n)}function UC(t,n){if(t!=null){if(yu(t))return LC(t,n);if(vu(t))return jC(t,n);if(_u(t))return VC(t,n);if(bu(t))return Iu(t,n);if(wu(t))return BC(t,n);if(xu(t))return HC(t,n)}throw Cu(t)}function st(t,n){return n?UC(t,n):et(t)}function J(...t){let n=or(t);return st(t,n)}function jl(t,n){let e=xe(t)?t:()=>t,i=r=>r.error(e());return new ye(n?r=>n.schedule(i,0,r):i)}function ia(t){return!!t&&(t instanceof ye||xe(t.lift)&&xe(t.subscribe))}var ra=Xa(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function zC(t){return t instanceof Date&&!isNaN(t)}function he(t,n){return De((e,i)=>{let r=0;e.subscribe(be(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:Bk}=Array;function Hk(t,n){return Bk(n)?t(...n):t(n)}function Tu(t){return he(n=>Hk(t,n))}var{isArray:Uk}=Array,{getPrototypeOf:zk,prototype:$k,keys:Gk}=Object;function Au(t){if(t.length===1){let n=t[0];if(Uk(n))return{args:n,keys:null};if(Wk(n)){let e=Gk(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function Wk(t){return t&&typeof t=="object"&&zk(t)===$k}function Ru(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function ar(...t){let n=or(t),e=gu(t),{args:i,keys:r}=Au(t);if(i.length===0)return st([],n);let o=new ye(qk(i,n,r?a=>Ru(r,a):$n));return e?o.pipe(Tu(e)):o}function qk(t,n,e=$n){return i=>{$C(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)$C(n,()=>{let c=st(t[l],n),u=!1;c.subscribe(be(i,m=>{o[l]=m,u||(u=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function $C(t,n,e){t?An(e,t,n):n()}function GC(t,n,e,i,r,o,a,s){let l=[],c=0,u=0,m=!1,v=()=>{m&&!l.length&&!c&&n.complete()},_=I=>c<i?y(I):l.push(I),y=I=>{o&&n.next(I),c++;let R=!1;et(e(I,u++)).subscribe(be(n,V=>{r?.(V),o?_(V):n.next(V)},()=>{R=!0},void 0,()=>{if(R)try{for(c--;l.length&&c<i;){let V=l.shift();a?An(n,a,()=>y(V)):y(V)}v()}catch(V){n.error(V)}}))};return t.subscribe(be(n,_,()=>{m=!0,v()})),()=>{s?.()}}function Zt(t,n,e=1/0){return xe(n)?Zt((i,r)=>he((o,a)=>n(i,o,r,a))(et(t(i,r))),e):(typeof n=="number"&&(e=n),De((i,r)=>GC(i,r,t,e)))}function mo(t=1/0){return Zt($n,t)}function WC(){return mo(1)}function po(...t){return WC()(st(t,or(t)))}function Ai(t){return new ye(n=>{et(t()).subscribe(n)})}function Bl(...t){let n=gu(t),{args:e,keys:i}=Au(t),r=new ye(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let u=0;u<a;u++){let m=!1;et(e[u]).subscribe(be(o,v=>{m||(m=!0,c--),s[u]=v},()=>l--,void 0,()=>{(!l||!m)&&(c||o.next(i?Ru(i,s):s),o.complete())}))}});return n?r.pipe(Tu(n)):r}function qC(t=0,n,e=AC){let i=-1;return n!=null&&(hu(n)?e=n:i=n),new ye(r=>{let o=zC(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function an(...t){let n=or(t),e=RC(t,1/0),i=t;return i.length?i.length===1?et(i[0]):mo(e)(st(i,n)):ft}function Ie(t,n){return De((e,i)=>{let r=0;e.subscribe(be(i,o=>t.call(n,o,r++)&&i.next(o)))})}function YC(t){return De((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(be(e,c=>{i=!0,r=c,o||et(t(c)).subscribe(o=be(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function rs(t,n=Vl){return YC(()=>qC(t,n))}function ho(t){return De((n,e)=>{let i=null,r=!1,o;i=n.subscribe(be(e,void 0,void 0,a=>{o=et(t(a,ho(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function oa(t,n){return xe(n)?Zt(t,n,1):Zt(t,1)}function Hl(t,n=Vl){return De((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}s()}e.subscribe(be(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function QC(t){return De((n,e)=>{let i=!1;n.subscribe(be(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function lt(t){return t<=0?()=>ft:De((n,e)=>{let i=0;n.subscribe(be(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function ku(t,n=$n){return t=t??Yk,De((e,i)=>{let r,o=!0;e.subscribe(be(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function Yk(t,n){return t===n}function ZC(t=Qk){return De((n,e)=>{let i=!1;n.subscribe(be(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function Qk(){return new ra}function aa(t){return De((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Nr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Ie((r,o)=>t(r,o,i)):$n,lt(1),e?QC(n):ZC(()=>new ra))}function Ou(t){return t<=0?()=>ft:De((n,e)=>{let i=[];n.subscribe(be(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Nu(){return De((t,n)=>{let e,i=!1;t.subscribe(be(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Ul(t={}){let{connector:n=()=>new k,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,u=!1,m=!1,v=()=>{s?.unsubscribe(),s=void 0},_=()=>{v(),a=l=void 0,u=m=!1},y=()=>{let I=a;_(),I?.unsubscribe()};return De((I,R)=>{c++,!m&&!u&&v();let V=l=l??n();R.add(()=>{c--,c===0&&!m&&!u&&(s=Kh(y,r))}),V.subscribe(R),!a&&c>0&&(a=new Or({next:_e=>V.next(_e),error:_e=>{m=!0,v(),s=Kh(_,e,_e),V.error(_e)},complete:()=>{u=!0,v(),s=Kh(_,i),V.complete()}}),et(I).subscribe(a))})(o)}}function Kh(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Or({next:()=>{i.unsubscribe(),t()}});return et(n(...e)).subscribe(i)}function Pu(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Ul({connector:()=>new co(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function zl(t){return Ie((n,e)=>t<=e)}function We(...t){let n=or(t);return De((e,i)=>{(n?po(t,e,n):po(t,e)).subscribe(i)})}function mt(t,n){return De((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(be(i,l=>{r?.unsubscribe();let c=0,u=o++;et(t(l,u)).subscribe(r=be(i,m=>i.next(n?n(l,m,u,c++):m),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function Te(t){return De((n,e)=>{et(t).subscribe(be(e,()=>e.complete(),Ol)),!e.closed&&n.subscribe(e)})}function Xh(t,n=!1){return De((e,i)=>{let r=0;e.subscribe(be(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function jt(t,n,e){let i=xe(t)||n||e?{next:t,error:n,complete:e}:t;return i?De((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(be(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):$n}var Jh;function Fu(){return Jh}function sr(t){let n=Jh;return Jh=t,n}var KC=Symbol("NotFound");function os(t){return t===KC||t?.name==="\u0275NotFound"}function eg(t,n,e){let i=Object.create(Zk);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Ko(i),ao(i),i.value===rr)throw i.error;return i.value};return o[bt]=i,Tl(i),o}function XC(t,n){Ko(t),Xo(t,n),Qa(t)}function JC(t,n){if(Ko(t),t.value===rr)throw t.error;nu(t,n),Qa(t)}var Zk=fe(D({},oo),{value:Qo,dirty:!0,error:null,equal:Al,kind:"linkedSignal",producerMustRecompute(t){return t.value===Qo||t.value===Zo},producerRecomputeValue(t){if(t.value===Zo)throw new Error("");let n=t.value;t.value=Zo;let e=kr(t),i,r=!1;try{let o=t.source(),a=n!==Qo&&n!==rr,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,ce(null),r=a&&i!==rr&&t.equal(n,i)}catch(o){i=rr,t.error=o}finally{so(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function eD(t){let n=ce(null);try{return t()}finally{ce(n)}}var zu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",P=class extends Error{code;constructor(n,e){super(lr(n,e)),this.code=n}};function Kk(t){return`NG0${Math.abs(t)}`}function lr(t,n){return`${Kk(t)}${n?": "+n:""}`}var ai=globalThis;function $e(t){for(let n in t)if(t[n]===$e)return n;throw Error("")}function oD(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Zl(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Zl).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function $u(t,n){return t?n?`${t} ${n}`:t:n||""}var Xk=$e({__forward_ref__:$e});function Rn(t){return t.__forward_ref__=Rn,t}function Nt(t){return mg(t)?t():t}function mg(t){return typeof t=="function"&&t.hasOwnProperty(Xk)&&t.__forward_ref__===Rn}function w(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function B(t){return{providers:t.providers||[],imports:t.imports||[]}}function Kl(t){return Jk(t,Gu)}function pg(t){return Kl(t)!==null}function Jk(t,n){return t.hasOwnProperty(n)&&t[n]||null}function e1(t){let n=t?.[Gu]??null;return n||null}function ng(t){return t&&t.hasOwnProperty(Vu)?t[Vu]:null}var Gu=$e({\u0275prov:$e}),Vu=$e({\u0275inj:$e}),C=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=w({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function hg(t){return t&&!!t.\u0275providers}var gg=$e({\u0275cmp:$e}),vg=$e({\u0275dir:$e}),_g=$e({\u0275pipe:$e}),yg=$e({\u0275mod:$e}),Gl=$e({\u0275fac:$e}),ua=$e({__NG_ELEMENT_ID__:$e}),tD=$e({__NG_ENV_ID__:$e});function bg(t){return Wu(t,"@NgModule"),t[yg]||null}function ki(t){return Wu(t,"@Component"),t[gg]||null}function Xl(t){return Wu(t,"@Directive"),t[vg]||null}function Cg(t){return Wu(t,"@Pipe"),t[_g]||null}function Wu(t,n){if(t==null)throw new P(-919,!1)}function ss(t){return typeof t=="string"?t:t==null?"":String(t)}var aD=$e({ngErrorCode:$e}),t1=$e({ngErrorMessage:$e}),n1=$e({ngTokenPath:$e});function Dg(t,n){return sD("",-200,n)}function qu(t,n){throw new P(-201,!1)}function sD(t,n,e){let i=new P(n,t);return i[aD]=n,i[t1]=t,e&&(i[n1]=e),i}function i1(t){return t[aD]}var ig;function lD(){return ig}function sn(t){let n=ig;return ig=t,n}function wg(t,n,e){let i=Kl(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;qu(t,"")}var r1={},sa=r1,o1="__NG_DI_FLAG__",rg=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=la(e)||0;try{return this.injector.get(n,i&8?null:sa,i)}catch(r){if(os(r))return r;throw r}}};function a1(t,n=0){let e=Fu();if(e===void 0)throw new P(-203,!1);if(e===null)return wg(t,void 0,n);{let i=s1(n),r=e.retrieve(t,i);if(os(r)){if(i.optional)return null;throw r}return r}}function se(t,n=0){return(lD()||a1)(Nt(t),n)}function d(t,n){return se(t,la(n))}function la(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function s1(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function og(t){let n=[];for(let e=0;e<t.length;e++){let i=Nt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new P(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=l1(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(se(r,o))}else n.push(se(i))}return n}function l1(t){return t[o1]}function go(t,n){let e=t.hasOwnProperty(Gl);return e?t[Gl]:null}function cD(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function dD(t){return t.flat(Number.POSITIVE_INFINITY)}function Yu(t,n){t.forEach(e=>Array.isArray(e)?Yu(e,n):n(e))}function Eg(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Jl(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function uD(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function fD(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Qu(t,n,e){let i=ls(t,n);return i>=0?t[i|1]=e:(i=~i,fD(t,i,n,e)),i}function Zu(t,n){let e=ls(t,n);if(e>=0)return t[e|1]}function ls(t,n){return c1(t,n,1)}function c1(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Oi={},Kt=[],cr=new C(""),xg=new C("",-1),Sg=new C(""),Wl=class{get(n,e=sa){if(e===sa){let r=sD("",-201);throw r.name="\u0275NotFound",r}return e}};function dr(t){return{\u0275providers:t}}function mD(t){return dr([{provide:cr,multi:!0,useValue:t}])}function pD(...t){return{\u0275providers:Ku(!0,t),\u0275fromNgModule:!0}}function Ku(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return Yu(n,a=>{let s=a;ju(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&hD(r,o),e}function hD(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Mg(r,o=>{n(o,i)})}}function ju(t,n,e,i){if(t=Nt(t),!t)return!1;let r=null,o=ng(t),a=!o&&ki(t);if(!o&&!a){let l=t.ngModule;if(o=ng(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)ju(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;Yu(o.imports,u=>{ju(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&hD(c,n)}if(!s){let c=go(r)||(()=>new r);n({provide:r,useFactory:c,deps:Kt},r),n({provide:Sg,useValue:r,multi:!0},r),n({provide:cr,useValue:()=>se(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;Mg(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Mg(t,n){for(let e of t)hg(e)&&(e=e.\u0275providers),Array.isArray(e)?Mg(e,n):n(e)}var d1=$e({provide:String,useValue:$e});function gD(t){return t!==null&&typeof t=="object"&&d1 in t}function u1(t){return!!(t&&t.useExisting)}function f1(t){return!!(t&&t.useFactory)}function ca(t){return typeof t=="function"}function vD(t){return!!t.useClass}var ec=new C(""),Lu={},nD={},tg;function cs(){return tg===void 0&&(tg=new Wl),tg}var Ge=class{},da=class extends Ge{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,sg(n,a=>this.processProvider(a)),this.records.set(xg,as(void 0,this)),r.has("environment")&&this.records.set(Ge,as(void 0,this));let o=this.records.get(ec);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Sg,Kt,{self:!0}))}retrieve(n,e){let i=la(e)||0;try{return this.get(n,sa,i)}catch(r){if(os(r))return r;throw r}}destroy(){$l(this),this._destroyed=!0;let n=ce(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ce(n)}}onDestroy(n){return $l(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){$l(this);let e=sr(this),i=sn(void 0),r;try{return n()}finally{sr(e),sn(i)}}get(n,e=sa,i){if($l(this),n.hasOwnProperty(tD))return n[tD](this);let r=la(i),o,a=sr(this),s=sn(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=v1(n)&&Kl(n);u&&this.injectableDefInScope(u)?c=as(ag(n),Lu):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?cs():this.parent;return e=r&8&&e===sa?null:e,l.get(n,e)}catch(l){let c=i1(l);throw c===-200||c===-201?new P(c,null):l}finally{sn(s),sr(a)}}resolveInjectorInitializers(){let n=ce(null),e=sr(this),i=sn(void 0),r;try{let o=this.get(cr,Kt,{self:!0});for(let a of o)a()}finally{sr(e),sn(i),ce(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Nt(n);let e=ca(n)?n:Nt(n&&n.provide),i=p1(n);if(!ca(n)&&n.multi===!0){let r=this.records.get(e);r||(r=as(void 0,Lu,!0),r.factory=()=>og(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ce(null);try{if(e.value===nD)throw Dg("");return e.value===Lu&&(e.value=nD,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&g1(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ce(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Nt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function ag(t){let n=Kl(t),e=n!==null?n.factory:go(t);if(e!==null)return e;if(t instanceof C)throw new P(-204,!1);if(t instanceof Function)return m1(t);throw new P(-204,!1)}function m1(t){if(t.length>0)throw new P(-204,!1);let e=e1(t);return e!==null?()=>e.factory(t):()=>new t}function p1(t){if(gD(t))return as(void 0,t.useValue);{let n=Ig(t);return as(n,Lu)}}function Ig(t,n,e){let i;if(ca(t)){let r=Nt(t);return go(r)||ag(r)}else if(gD(t))i=()=>Nt(t.useValue);else if(f1(t))i=()=>t.useFactory(...og(t.deps||[]));else if(u1(t))i=(r,o)=>se(Nt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Nt(t&&(t.useClass||t.provide));if(h1(t))i=()=>new r(...og(t.deps));else return go(r)||ag(r)}return i}function $l(t){if(t.destroyed)throw new P(-205,!1)}function as(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function h1(t){return!!t.deps}function g1(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function v1(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function sg(t,n){for(let e of t)Array.isArray(e)?sg(e,n):e&&hg(e)?sg(e.\u0275providers,n):n(e)}function Bt(t,n){let e;t instanceof da?($l(t),e=t):e=new rg(t);let i,r=sr(e),o=sn(void 0);try{return n()}finally{sr(r),sn(o)}}function _D(){return lD()!==void 0||Fu()!=null}var cn=0,ie=1,ge=2,Dt=3,si=4,dn=5,un=6,ds=7,wt=8,Gn=9,Ni=10,Le=11,us=12,Tg=13,fa=14,Ut=15,yo=16,ma=17,ur=18,Fr=19,Ag=20,Pr=21,Xu=22,vo=23,Wn=24,pa=25,bo=26,Ne=27,yD=1,Pi=6,fr=7,tc=8,ha=9,pt=10;function li(t){return Array.isArray(t)&&typeof t[yD]=="object"}function qn(t){return Array.isArray(t)&&t[yD]===!0}function Rg(t){return(t.flags&4)!==0}function mr(t){return t.componentOffset>-1}function fs(t){return(t.flags&1)===1}function Fi(t){return!!t.template}function ga(t){return(t[ge]&512)!==0}function Co(t){return(t[ge]&256)===256}var kg="svg",bD="math";function Yn(t){for(;Array.isArray(t);)t=t[cn];return t}function Og(t,n){return Yn(n[t])}function ci(t,n){return Yn(n[t.index])}function ms(t,n){return t.data[n]}function Ju(t,n){return t[n]}function Ng(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function di(t,n){let e=n[t];return li(e)?e:e[cn]}function CD(t){return(t[ge]&4)===4}function ef(t){return(t[ge]&128)===128}function DD(t){return qn(t[Dt])}function Qn(t,n){return n==null?null:t[n]}function Pg(t){t[ma]=0}function Fg(t){t[ge]&1024||(t[ge]|=1024,ef(t)&&va(t))}function wD(t,n){for(;t>0;)n=n[fa],t--;return n}function nc(t){return!!(t[ge]&9216||t[Wn]?.dirty)}function tf(t){t[Ni].changeDetectionScheduler?.notify(8),t[ge]&64&&(t[ge]|=1024),nc(t)&&va(t)}function va(t){t[Ni].changeDetectionScheduler?.notify(0);let n=_o(t);for(;n!==null&&!(n[ge]&8192||(n[ge]|=8192,!ef(n)));)n=_o(n)}function Lg(t,n){if(Co(t))throw new P(911,!1);t[Pr]===null&&(t[Pr]=[]),t[Pr].push(n)}function ED(t,n){if(t[Pr]===null)return;let e=t[Pr].indexOf(n);e!==-1&&t[Pr].splice(e,1)}function _o(t){let n=t[Dt];return qn(n)?n[Dt]:n}function Vg(t){return t[ds]??=[]}function jg(t){return t.cleanup??=[]}function xD(t,n,e,i){let r=Vg(n);r.push(e),t.firstCreatePass&&jg(t).push(i,r.length-1)}var Se={lFrame:VD(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var lg=!1;function SD(){return Se.lFrame.elementDepthCount}function MD(){Se.lFrame.elementDepthCount++}function Bg(){Se.lFrame.elementDepthCount--}function nf(){return Se.bindingsEnabled}function rf(){return Se.skipHydrationRootTNode!==null}function Hg(t){return Se.skipHydrationRootTNode===t}function ID(t){Se.skipHydrationRootTNode=t}function Ug(){Se.skipHydrationRootTNode=null}function le(){return Se.lFrame.lView}function nt(){return Se.lFrame.tView}function Ae(t){return Se.lFrame.contextLView=t,t[wt]}function Re(t){return Se.lFrame.contextLView=null,t}function Pt(){let t=zg();for(;t!==null&&t.type===64;)t=t.parent;return t}function zg(){return Se.lFrame.currentTNode}function TD(){let t=Se.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function ps(t,n){let e=Se.lFrame;e.currentTNode=t,e.isParent=n}function $g(){return Se.lFrame.isParent}function Gg(){Se.lFrame.isParent=!1}function AD(){return Se.lFrame.contextLView}function Wg(){return lg}function ql(t){let n=lg;return lg=t,n}function hs(){let t=Se.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function RD(){return Se.lFrame.bindingIndex}function kD(t){return Se.lFrame.bindingIndex=t}function Do(){return Se.lFrame.bindingIndex++}function of(t){let n=Se.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function OD(){return Se.lFrame.inI18n}function ND(t,n){let e=Se.lFrame;e.bindingIndex=e.bindingRootIndex=t,af(n)}function PD(){return Se.lFrame.currentDirectiveIndex}function af(t){Se.lFrame.currentDirectiveIndex=t}function FD(t){let n=Se.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function sf(){return Se.lFrame.currentQueryIndex}function ic(t){Se.lFrame.currentQueryIndex=t}function _1(t){let n=t[ie];return n.type===2?n.declTNode:n.type===1?t[dn]:null}function qg(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=_1(o),r===null||(o=o[fa],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=Se.lFrame=LD();return i.currentTNode=n,i.lView=t,!0}function lf(t){let n=LD(),e=t[ie];Se.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function LD(){let t=Se.lFrame,n=t===null?null:t.child;return n===null?VD(t):n}function VD(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function jD(){let t=Se.lFrame;return Se.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Yg=jD;function cf(){let t=jD();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function BD(t){return(Se.lFrame.contextLView=wD(t,Se.lFrame.contextLView))[wt]}function pr(){return Se.lFrame.selectedIndex}function wo(t){Se.lFrame.selectedIndex=t}function rc(){let t=Se.lFrame;return ms(t.tView,t.selectedIndex)}function hr(){Se.lFrame.currentNamespace=kg}function df(){return Se.lFrame.currentNamespace}var HD=!0;function uf(){return HD}function Lr(t){HD=t}function cg(t,n=null,e=null,i){let r=Qg(t,n,e,i);return r.resolveInjectorInitializers(),r}function Qg(t,n=null,e=null,i,r=new Set){let o=[e||Kt,pD(t)],a;return new da(o,n||cs(),a||null,r)}var oe=class t{static THROW_IF_NOT_FOUND=sa;static NULL=new Wl;static create(n,e){if(Array.isArray(n))return cg({name:""},e,n,"");{let i=n.name??"";return cg({name:i},n.parent,n.providers,i)}}static \u0275prov=w({token:t,providedIn:"any",factory:()=>se(xg)});static __NG_ELEMENT_ID__=-1},j=new C(""),fn=(()=>{class t{static __NG_ELEMENT_ID__=y1;static __NG_ENV_ID__=e=>e}return t})(),Bu=class extends fn{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Co(this._lView)}onDestroy(n){let e=this._lView;return Lg(e,n),()=>ED(e,n)}};function y1(){return new Bu(le())}var UD=!1,zD=new C(""),gr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Ct(!1);debugTaskTracker=d(zD,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ye(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),dg=class extends k{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,_D()&&(this.destroyRef=d(fn,{optional:!0})??void 0,this.pendingTasks=d(gr,{optional:!0})??void 0)}emit(n){let e=ce(null);try{super.next(n)}finally{ce(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof pe&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Z=dg;function Hu(...t){}function Zg(t){let n,e;function i(){t=Hu;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function $D(t){return queueMicrotask(()=>t()),()=>{t=Hu}}var Kg="isAngularZone",Yl=Kg+"_ID",b1=0,re=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Z(!1);onMicrotaskEmpty=new Z(!1);onStable=new Z(!1);onError=new Z(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=UD}=n;if(typeof Zone>"u")throw new P(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,w1(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Kg)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new P(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new P(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,C1,Hu,Hu);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},C1={};function Xg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function D1(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Zg(()=>{t.callbackScheduled=!1,ug(t),t.isCheckStableRunning=!0,Xg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),ug(t)}function w1(t){let n=()=>{D1(t)},e=b1++;t._inner=t._inner.fork({name:"angular",properties:{[Kg]:!0,[Yl]:e,[Yl+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(E1(l))return i.invokeTask(o,a,s,l);try{return iD(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),rD(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return iD(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!x1(l)&&n(),rD(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,ug(t),Xg(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function ug(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function iD(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function rD(t){t._nesting--,Xg(t)}var Ql=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Z;onMicrotaskEmpty=new Z;onStable=new Z;onError=new Z;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function E1(t){return GD(t,"__ignore_ng_zone__")}function x1(t){return GD(t,"__scheduler_tick__")}function GD(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var ln=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Zn=new C("",{factory:()=>{let t=d(re),n=d(Ge),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(ln),e.handleError(i))})}}}),WD={provide:cr,useValue:()=>{let t=d(ln,{optional:!0})},multi:!0},S1=new C("",{factory:()=>{let t=d(j).defaultView;if(!t)return;let n=d(Zn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(fn).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Jg(){return dr([mD(()=>{d(S1)})])}function S(t,n){let[e,i,r]=Fh(t,n?.equal),o=e,a=o[bt];return o.set=i,o.update=r,o.asReadonly=ff.bind(o),o}function ff(){let t=this[bt];if(t.readonlyFn===void 0){let n=()=>this();n[bt]=t,t.readonlyFn=n}return t.readonlyFn}var gs=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=M1}return t})();function M1(){return new gs(le(),Pt())}var Ri=class{},oc=new C("",{factory:()=>!0});var ev=new C(""),vs=(()=>{class t{internalPendingTasks=d(gr);scheduler=d(Ri);errorHandler=d(Zn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),mf=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>new fg})}return t})(),fg=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Uu=class{[bt];constructor(n){this[bt]=n}destroy(){this[bt].destroy()}};function Li(t,n){let e=n?.injector??d(oe),i=n?.manualCleanup!==!0?e.get(fn):null,r,o=e.get(gs,null,{optional:!0}),a=e.get(Ri);return o!==null?(r=A1(o.view,a,t),i instanceof Bu&&i._lView===o.view&&(i=null)):r=R1(t,e.get(mf),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Uu(r)}var qD=fe(D({},Lh),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=ql(!1);try{Vh(this)}finally{ql(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ce(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ce(t)}}}),I1=fe(D({},qD),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(lo(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),T1=fe(D({},qD),{consumerMarkedDirty(){this.view[ge]|=8192,va(this.view),this.notifier.notify(13)},destroy(){if(lo(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[vo]?.delete(this)}});function A1(t,n,e){let i=Object.create(T1);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=YD(i,e),t[vo]??=new Set,t[vo].add(i),i.consumerMarkedDirty(i),i}function R1(t,n,e){let i=Object.create(I1);return i.fn=YD(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function YD(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}var pf={JSACTION:"jsaction"};function _c(t){return{toString:t}.toString()}function j1(t){return typeof t=="function"}function Bw(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Sf=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Pe=(()=>{let t=()=>Hw;return t.ngInherit=!0,t})();function Hw(t){return t.type.prototype.ngOnChanges&&(t.setInput=H1),B1}function B1(){let t=zw(this),n=t?.current;if(n){let e=t.previous;if(e===Oi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function H1(t,n,e,i,r){let o=this.declaredInputs[i],a=zw(t)||U1(t,{previous:Oi,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Sf(c&&c.currentValue,e,l===Oi),Bw(t,n,r,e)}var Uw="__ngSimpleChanges__";function zw(t){return t[Uw]||null}function U1(t,n){return t[Uw]=n}var QD=[];var Be=function(t,n=null,e){for(let i=0;i<QD.length;i++){let r=QD[i];r(t,n,e)}},Oe=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Oe||{});function z1(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=Hw(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function $w(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function yf(t,n,e){Gw(t,n,3,e)}function bf(t,n,e,i){(t[ge]&3)===e&&Gw(t,n,e,i)}function tv(t,n){let e=t[ge];(e&3)===n&&(e&=16383,e+=1,t[ge]=e)}function Gw(t,n,e,i){let r=i!==void 0?t[ma]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[ma]+=65536),(s<o||o==-1)&&($1(t,e,n,l),t[ma]=(t[ma]&4294901760)+l+2),l++}function ZD(t,n){Be(Oe.LifecycleHookStart,t,n);let e=ce(null);try{n.call(t)}finally{ce(e),Be(Oe.LifecycleHookEnd,t,n)}}function $1(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[ge]>>14<t[ma]>>16&&(t[ge]&3)===n&&(t[ge]+=16384,ZD(s,o)):ZD(s,o)}var ys=-1,ba=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function G1(t){return(t.flags&8)!==0}function W1(t){return(t.flags&16)!==0}function q1(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];Y1(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function Ww(t){return t===3||t===4||t===6}function Y1(t){return t.charCodeAt(0)===64}function Ds(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?KD(t,e,r,null,n[++i]):KD(t,e,r,null,null))}}return t}function KD(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function qw(t){return t!==ys}function Mf(t){return t&32767}function Q1(t){return t>>16}function If(t,n){let e=Q1(t),i=n;for(;e>0;)i=i[fa],e--;return i}var hv=!0;function Tf(t){let n=hv;return hv=t,n}var Z1=256,Yw=Z1-1,Qw=5,K1=0,vr={};function X1(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(ua)&&(i=e[ua]),i==null&&(i=e[ua]=K1++);let r=i&Yw,o=1<<r;n.data[t+(r>>Qw)]|=o}function Af(t,n){let e=Zw(t,n);if(e!==-1)return e;let i=n[ie];i.firstCreatePass&&(t.injectorIndex=n.length,nv(i.data,t),nv(n,null),nv(i.blueprint,null));let r=t_(t,n),o=t.injectorIndex;if(qw(r)){let a=Mf(r),s=If(r,n),l=s[ie].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function nv(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Zw(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function t_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=tE(r),i===null)return ys;if(e++,r=r[fa],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return ys}function gv(t,n,e){X1(t,n,e)}function J1(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Ww(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Kw(t,n,e){if(e&8||t!==void 0)return t;qu(n,"NodeInjector")}function Xw(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Gn],o=sn(void 0);try{return r?r.get(n,i,e&8):wg(n,i,e&8)}finally{sn(o)}}return Kw(i,n,e)}function Jw(t,n,e,i=0,r){if(t!==null){if(n[ge]&2048&&!(i&2)){let a=iO(t,n,e,i,vr);if(a!==vr)return a}let o=eE(t,n,e,i,vr);if(o!==vr)return o}return Xw(n,e,i,r)}function eE(t,n,e,i,r){let o=tO(e);if(typeof o=="function"){if(!qg(n,t,i))return i&1?Kw(r,e,i):Xw(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))qu(e);else return a}finally{Yg()}}else if(typeof o=="number"){let a=null,s=Zw(t,n),l=ys,c=i&1?n[Ut][dn]:null;for((s===-1||i&4)&&(l=s===-1?t_(t,n):n[s+8],l===ys||!JD(i,!1)?s=-1:(a=n[ie],s=Mf(l),n=If(l,n)));s!==-1;){let u=n[ie];if(XD(o,s,u.data)){let m=eO(s,n,e,a,i,c);if(m!==vr)return m}l=n[s+8],l!==ys&&JD(i,n[ie].data[s+8]===c)&&XD(o,s,n)?(a=u,s=Mf(l),n=If(l,n)):s=-1}}return r}function eO(t,n,e,i,r,o){let a=n[ie],s=a.data[t+8],l=i==null?mr(s)&&hv:i!=a&&(s.type&3)!==0,c=r&1&&o===s,u=Cf(s,a,e,l,c);return u!==null?dc(n,a,u,s,r):vr}function Cf(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,m=i?s:s+u,v=r?s+u:c;for(let _=m;_<v;_++){let y=a[_];if(_<l&&e===y||_>=l&&y.type===e)return _}if(r){let _=a[l];if(_&&Fi(_)&&_.type===e)return l}return null}function dc(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof ba){let s=o;if(s.resolving)throw Dg("");let l=Tf(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],u,m=s.injectImpl?sn(s.injectImpl):null,v=qg(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&z1(e,a[e],n)}finally{m!==null&&sn(m),Tf(l),s.resolving=!1,Yg()}}return o}function tO(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(ua)?t[ua]:void 0;return typeof n=="number"?n>=0?n&Yw:nO:n}function XD(t,n,e){let i=1<<t;return!!(e[n+(t>>Qw)]&i)}function JD(t,n){return!(t&2)&&!(t&1&&n)}var ya=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Jw(this._tNode,this._lView,n,la(i),e)}};function nO(){return new ya(Pt(),le())}function je(t){return _c(()=>{let n=t.prototype.constructor,e=n[Gl]||vv(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Gl]||vv(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function vv(t){return mg(t)?()=>{let n=vv(Nt(t));return n&&n()}:go(t)}function iO(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[ge]&2048&&!ga(a);){let s=eE(o,a,e,i|2,vr);if(s!==vr)return s;let l=o.parent;if(!l){let c=a[Ag];if(c){let u=c.get(e,vr,i&-5);if(u!==vr)return u}l=tE(a),a=a[fa]}o=l}return r}function tE(t){let n=t[ie],e=n.type;return e===2?n.declTNode:e===1?t[dn]:null}function yc(t){return J1(Pt(),t)}function rO(){return Ss(Pt(),le())}function Ss(t,n){return new W(ci(t,n))}var W=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=rO}return t})();function nE(t){return t instanceof W?t.nativeElement:t}function oO(){return this._results[Symbol.iterator]()}var Xt=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new k}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=dD(n);(this._changesDetected=!cD(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=oO},iE="ngSkipHydration",aO="ngskiphydration";function rE(t){let n=t.mergedAttrs;if(n===null)return!1;for(let e=0;e<n.length;e+=2){let i=n[e];if(typeof i=="number")return!1;if(typeof i=="string"&&i.toLowerCase()===aO)return!0}return!1}function oE(t){return t.hasAttribute(iE)}function Rf(t){return(t.flags&128)===128}function aE(t){if(Rf(t))return!0;let n=t.parent;for(;n;){if(Rf(t)||rE(n))return!0;n=n.parent}return!1}var n_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(n_||{}),sE=new Map,sO=0;function lO(){return sO++}function cO(t){sE.set(t[Fr],t)}function _v(t){sE.delete(t[Fr])}var ew="__ngContext__";function ws(t,n){li(n)?(t[ew]=n[Fr],cO(n)):t[ew]=n}function lE(t){return dE(t[us])}function cE(t){return dE(t[si])}function dE(t){for(;t!==null&&!qn(t);)t=t[si];return t}var yv;function i_(t){yv=t}function uE(){if(yv!==void 0)return yv;if(typeof document<"u")return document;throw new P(210,!1)}var ui=new C("",{factory:()=>dO}),dO="ng";var $f=new C(""),jr=new C("",{providedIn:"platform",factory:()=>"unknown"}),bc=new C(""),Ea=new C("",{factory:()=>d(j).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ms=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>{let e=new t;return e.store=uO(d(j),d(ui)),e}});store={};onSerializeCallbacks={};get(e,i){return this.store[e]!==void 0?this.store[e]:i}set(e,i){this.store[e]=i}remove(e){delete this.store[e]}hasKey(e){return this.store.hasOwnProperty(e)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(e,i){this.onSerializeCallbacks[e]=i}toJson(){for(let e in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(e))try{this.store[e]=this.onSerializeCallbacks[e]()}catch(i){console.warn("Exception in onSerialize callback: ",i)}return JSON.stringify(this.store).replace(/</g,"\\u003C").replace(/\//g,"\\u002F")}}return t})();function uO(t,n){let e=t.getElementById(n+"-state");if(e?.textContent)try{return JSON.parse(e.textContent)}catch(i){console.warn("Exception while restoring TransferState for app "+n,i)}return{}}var fE="h",mE="b",fO="f",mO="n",pE="e",hE="t",Gf="c",r_="x",uc="r",gE="i",vE="n",o_="d";var _E="di",yE="s",bE="p";var Is=new C(""),CE=!1,a_=new C("",{factory:()=>CE});var s_=new C(""),DE=!1,wE=new C("",{factory:()=>[]}),EE=new C(""),l_=new C("",{factory:()=>new Map});var Cc="ngb";var xE=(t,n,e)=>{let i=t,r=i.__jsaction_fns??new Map,o=r.get(n)??[];o.push(e),r.set(n,o),i.__jsaction_fns=r},SE=(t,n)=>{let e=t,i=e.getAttribute(Cc)??"",r=n.get(i)??new Set;r.has(e)||r.add(e),n.set(i,r)};var ME=t=>{t.removeAttribute(pf.JSACTION),t.removeAttribute(Cc),t.__jsaction_fns=void 0},IE=new C("",{factory:()=>({})}),bv=new WeakMap;function pO(t,n){if(t==null||typeof t!="object")return;let e=bv.get(t);e||(e=new WeakSet,bv.set(t,e)),e.add(n)}function c_(t,n){let e=n?.__jsaction_fns?.get(t.type);if(!(!e||!n?.isConnected)&&!(n&&bv.get(t)?.has(n)))for(let i of e)i(t)}var Cv=new Map;function TE(t,n){return Cv.set(t,n),()=>Cv.delete(t)}var tw=!1,AE=(t,n,e,i)=>{};function hO(t,n,e,i){AE(t,n,e,i)}function RE(){tw||(AE=(t,n,e,i)=>{let r=t[Gn].get(ui);Cv.get(r)?.(n,e,i)},tw=!0)}var Wf=new C("");function Dc(t){return(t.flags&32)===32}var gO="__nghData__",d_=gO,vO="__nghDeferData__",kE=vO;var Df="ngh",OE="nghm",NE=()=>null;function _O(t,n,e=!1){let i=t.getAttribute(Df);if(i==null)return null;let[r,o]=i.split("|");if(i=e?o:r,!i)return null;let a=o?`|${o}`:"",s=e?r:a,l={};if(i!==""){let u=n.get(Ms,null,{optional:!0});u!==null&&(l=u.get(d_,[])[Number(i)])}let c={data:l,firstChild:t.firstChild??null};return e&&(c.firstChild=t,qf(c,0,t.nextSibling)),s?t.setAttribute(Df,s):t.removeAttribute(Df),c}function PE(){NE=_O}function FE(t,n,e=!1){return NE(t,n,e)}function LE(t){let n=t._lView;return n[ie].type===2?null:(ga(n)&&(n=n[Ne]),n)}function yO(t){return t.textContent?.replace(/\s/gm,"")}function bO(t){let n=uE(),e=n.createNodeIterator(t,NodeFilter.SHOW_COMMENT,{acceptNode(o){let a=yO(o);return a==="ngetn"||a==="ngtns"?NodeFilter.FILTER_ACCEPT:NodeFilter.FILTER_REJECT}}),i,r=[];for(;i=e.nextNode();)r.push(i);for(let o of r)o.textContent==="ngetn"?o.replaceWith(n.createTextNode("")):o.remove()}function qf(t,n,e){t.segmentHeads??={},t.segmentHeads[n]=e}function Dv(t,n){return t.segmentHeads?.[n]??null}function VE(t){return t.get(EE,!1,{optional:!0})}function CO(t,n){let e=t.data,i=e[pE]?.[n]??null;return i===null&&e[Gf]?.[n]&&(i=u_(t,n)),i}function jE(t,n){return t.data[Gf]?.[n]??null}function u_(t,n){let e=jE(t,n)??[],i=0;for(let r of e)i+=r[uc]*(r[r_]??1);return i}function DO(t){if(typeof t.disconnectedNodes>"u"){let n=t.data[o_];t.disconnectedNodes=n?new Set(n):null}return t.disconnectedNodes}function BE(t,n){if(typeof t.disconnectedNodes>"u"){let e=t.data[o_];t.disconnectedNodes=e?new Set(e):null}return!!DO(t)?.has(n)}function Yf(t,n){let e=t[un];return e!==null&&!rf()&&!Dc(n)&&!BE(e,n.index-Ne)}function wO(t,n){let e=n.get(Wf),r=n.get(Ms).get(kE,{}),o=!1,a=t,s=null,l=[];for(;!o&&a;){o=e.has(a);let c=e.hydrating.get(a);if(s===null&&c!=null){s=c.promise;break}l.unshift(a),a=r[a][bE]}return{parentBlockPromise:s,hydrationQueue:l}}function iv(t){return!!t&&t.nodeType===Node.COMMENT_NODE&&t.textContent?.trim()===OE}function nw(t){for(;t&&t.nodeType===Node.TEXT_NODE;)t=t.previousSibling;return t}function HE(t){for(let i of t.body.childNodes)if(iv(i))return;let n=nw(t.body.previousSibling);if(iv(n))return;let e=nw(t.head.lastChild);if(!iv(e))throw new P(-507,!1)}function UE(t,n){let e=t.contentQueries;if(e!==null){let i=ce(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];ic(o),s.contentQueries(2,n[a],a)}}}finally{ce(i)}}}function wv(t,n,e){ic(0);let i=ce(null);try{n(t,e)}finally{ce(i)}}function f_(t,n,e){if(Rg(n)){let i=ce(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{ce(i)}}}var Bi=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Bi||{});var hf;function EO(){if(hf===void 0&&(hf=null,ai.trustedTypes))try{hf=ai.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return hf}function Qf(t){return EO()?.createHTML(t)||t}var gf;function xO(){if(gf===void 0&&(gf=null,ai.trustedTypes))try{gf=ai.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return gf}function iw(t){return xO()?.createScriptURL(t)||t}var Vr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${zu})`}},Ev=class extends Vr{getTypeName(){return"HTML"}},xv=class extends Vr{getTypeName(){return"Style"}},Sv=class extends Vr{getTypeName(){return"Script"}},Mv=class extends Vr{getTypeName(){return"URL"}},Iv=class extends Vr{getTypeName(){return"ResourceURL"}};function Hi(t){return t instanceof Vr?t.changingThisBreaksApplicationSecurity:t}function Br(t,n){let e=zE(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${zu})`)}return e===n}function zE(t){return t instanceof Vr&&t.getTypeName()||null}function m_(t){return new Ev(t)}function p_(t){return new xv(t)}function h_(t){return new Sv(t)}function g_(t){return new Mv(t)}function v_(t){return new Iv(t)}function SO(t){let n=new Av(t);return MO()?new Tv(n):n}var Tv=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Qf(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Av=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Qf(n),e}};function MO(){try{return!!new window.DOMParser().parseFromString(Qf(""),"text/html")}catch{return!1}}var IO=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function wc(t){return t=String(t),t.match(IO)?t:"unsafe:"+t}function Hr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Ec(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var $E=Hr("area,br,col,hr,img,wbr"),GE=Hr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),WE=Hr("rp,rt"),TO=Ec(WE,GE),AO=Ec(GE,Hr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),RO=Ec(WE,Hr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),rw=Ec($E,AO,RO,TO),qE=Hr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),kO=Hr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),OO=Hr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),NO=Ec(qE,kO,OO),PO=Hr("script,style,template"),Rv=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=VO(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=LO(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=ow(n).toLowerCase();if(!rw.hasOwnProperty(e))return this.sanitizedSomething=!0,!PO.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!NO.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;qE[s]&&(l=wc(l)),this.buf.push(" ",a,'="',aw(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=ow(n).toLowerCase();rw.hasOwnProperty(e)&&!$E.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(aw(n))}};function FO(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function LO(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw YE(n);return n}function VO(t){let n=t.firstChild;if(n&&FO(t,n))throw YE(n);return n}function ow(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function YE(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var jO=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,BO=/([^\#-~ |!])/g;function aw(t){return t.replace(/&/g,"&amp;").replace(jO,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(BO,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var vf;function __(t,n){let e=null;try{vf=vf||SO(t);let i=n?String(n):"";e=vf.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=vf.getInertBodyElement(i)}while(i!==o);let s=new Rv().sanitizeChildren(sw(e)||e);return Qf(s)}finally{if(e){let i=sw(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function sw(t){return"content"in t&&HO(t)?t.content:null}function HO(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var UO=/^>|^->|<!--|-->|--!>|<!-$/g,zO=/(<|>)/g,$O="\u200B$1\u200B";function GO(t){return t.replace(UO,n=>n.replace(zO,$O))}function QE(t,n){return t.createText(n)}function WO(t,n,e){t.setValue(n,e)}function ZE(t,n){return t.createComment(GO(n))}function y_(t,n,e){return t.createElement(n,e)}function kf(t,n,e,i,r){t.insertBefore(n,e,i,r)}function KE(t,n,e){t.appendChild(n,e)}function lw(t,n,e,i,r){i!==null?kf(t,n,e,i,r):KE(t,n,e)}function b_(t,n,e,i){t.removeChild(null,n,e,i)}function XE(t){t.textContent=""}function qO(t,n,e){t.setAttribute(n,"style",e)}function YO(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function JE(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&q1(t,n,i),r!==null&&YO(t,n,r),o!==null&&qO(t,n,o)}var Ft=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Ft||{});function ex(t){let n=nx();return n?n.sanitize(Ft.URL,t)||"":Br(t,"URL")?Hi(t):wc(ss(t))}function tx(t){let n=nx();if(n)return iw(n.sanitize(Ft.RESOURCE_URL,t)||"");if(Br(t,"ResourceURL"))return iw(Hi(t));throw new P(904,!1)}var QO={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function ZO(t,n){return QO[t.toLowerCase()]?.[n.toLowerCase()]===!0?tx:ex}function C_(t,n,e){return ZO(n,e)(t)}function nx(){let t=le();return t&&t[Ni].sanitizer}function ix(t){return t.ownerDocument.body}function rx(t){return t instanceof Function?t():t}function KO(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var ox="ng-template";function XO(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&KO(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(D_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function D_(t){return t.type===4&&t.value!==ox}function JO(t,n,e){let i=t.type===4&&!e?ox:t.value;return n===i}function eN(t,n,e){let i=4,r=t.attrs,o=r!==null?iN(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!Vi(i)&&!Vi(l))return!1;if(a&&Vi(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!JO(t,l,e)||l===""&&n.length===1){if(Vi(i))return!1;a=!0}}else if(i&8){if(r===null||!XO(t,r,l,e)){if(Vi(i))return!1;a=!0}}else{let c=n[++s],u=tN(l,r,D_(t),e);if(u===-1){if(Vi(i))return!1;a=!0;continue}if(c!==""){let m;if(u>o?m="":m=r[u+1].toLowerCase(),i&2&&c!==m){if(Vi(i))return!1;a=!0}}}}return Vi(i)||a}function Vi(t){return(t&1)===0}function tN(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return rN(n,t)}function ax(t,n,e=!1){for(let i=0;i<n.length;i++)if(eN(t,n[i],e))return!0;return!1}function nN(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function iN(t){for(let n=0;n<t.length;n++){let e=t[n];if(Ww(e))return n}return t.length}function rN(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function oN(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function cw(t,n){return t?":not("+n.trim()+")":n}function aN(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!Vi(a)&&(n+=cw(o,r),r=""),i=a,o=o||!Vi(i);e++}return r!==""&&(n+=cw(o,r)),n}function sN(t){return t.map(aN).join(",")}function lN(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Vi(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var mn={};function w_(t,n,e,i,r,o,a,s,l,c,u){let m=Ne+i,v=m+r,_=cN(m,v),y=typeof c=="function"?c():c;return _[ie]={type:t,blueprint:_,template:e,queries:null,viewQuery:s,declTNode:n,data:_.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:v,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:y,incompleteFirstPass:!1,ssrId:u}}function cN(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:mn);return e}function dN(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=w_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function E_(t,n,e,i,r,o,a,s,l,c,u){let m=n.blueprint.slice();return m[cn]=r,m[ge]=i|4|128|8|64|1024,(c!==null||t&&t[ge]&2048)&&(m[ge]|=2048),Pg(m),m[Dt]=m[fa]=t,m[wt]=e,m[Ni]=a||t&&t[Ni],m[Le]=s||t&&t[Le],m[Gn]=l||t&&t[Gn]||null,m[dn]=o,m[Fr]=lO(),m[un]=u,m[Ag]=c,m[Ut]=n.type==2?t[Ut]:m,m}function uN(t,n,e){let i=ci(n,t),r=dN(e),o=t[Ni].rendererFactory,a=x_(t,E_(t,r,null,sx(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function sx(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function lx(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function x_(t,n){return t[us]?t[Tg][si]=n:t[us]=n,t[Tg]=n,n}function h(t=1){cx(nt(),le(),pr()+t,!1)}function cx(t,n,e,i){if(!i)if((n[ge]&3)===3){let o=t.preOrderCheckHooks;o!==null&&yf(n,o,e)}else{let o=t.preOrderHooks;o!==null&&bf(n,o,0,e)}wo(e)}var Zf=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Zf||{});function kv(t,n,e,i){let r=ce(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Zf.SignalBased)!==0&&(l=n[o][bt]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):Bw(n,l,o,i)}finally{ce(r)}}var _r=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(_r||{}),fN;function S_(t,n){return fN(t,n)}var cK=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Ov=new WeakMap,sc=new WeakSet;function mN(t,n){let e=Ov.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),sc.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function pN(t,n){let e=Ov.get(t);e?e.includes(n)||e.push(n):Ov.set(t,[n])}var Ca=new Set,Kf=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Kf||{}),Ui=new C(""),dw=new Set;function fi(t){dw.has(t)||(dw.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Xf=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),M_=[0,1,2,3],I_=(()=>{class t{ngZone=d(re);scheduler=d(Ri);errorHandler=d(ln,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Ui,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Be(Oe.AfterRenderHooksStart),this.executing=!0;for(let i of M_)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Be(Oe.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[pa]??=[]).push(e),va(i),i[ge]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Kf.AFTER_NEXT_RENDER,e):e()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),fc=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[pa];n&&(this.view[pa]=n.filter(e=>e!==this))}};function ht(t,n){let e=n?.injector??d(oe);return fi("NgAfterNextRender"),gN(t,e,n,!0)}function hN(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function gN(t,n,e,i){let r=n.get(Xf);r.impl??=n.get(I_);let o=n.get(Ui,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(fn):null,s=n.get(gs,null,{optional:!0}),l=new fc(r.impl,hN(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var dx=new C("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(Ge)})});function ux(t,n,e){let i=t.get(dx);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function vN(t,n){let e=t.get(dx);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function _N(t,n){for(let[e,i]of n)ux(t,i.animateFns)}function uw(t,n,e,i){let r=t?.[bo]?.enter;n!==null&&r&&r.has(e.index)&&_N(i,r)}function _s(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;qn(r)?l=r:li(r)&&(c=!0,r=r[cn]);let u=Yn(r);t===0&&i!==null?(uw(s,i,o,e),a==null?KE(n,i,u):kf(n,i,u,a||null,!0)):t===1&&i!==null?(uw(s,i,o,e),kf(n,i,u,a||null,!0),mN(o,u)):t===2?(s?.[bo]?.leave?.has(o.index)&&pN(o,u),sc.delete(u),fw(s,o,e,m=>{if(sc.has(u)){sc.delete(u);return}b_(n,u,c,m)})):t===3&&(sc.delete(u),fw(s,o,e,()=>{n.destroyNode(u)})),l!=null&&TN(n,t,e,l,o,i,a)}}function yN(t,n){fx(t,n),n[cn]=null,n[dn]=null}function bN(t,n,e,i,r,o){i[cn]=r,i[dn]=n,em(t,i,e,1,r,o)}function fx(t,n){n[Ni].changeDetectionScheduler?.notify(9),em(t,n,n[Le],2,null,null)}function CN(t){let n=t[us];if(!n)return rv(t[ie],t);for(;n;){let e=null;if(li(n))e=n[us];else{let i=n[pt];i&&(e=i)}if(!e){for(;n&&!n[si]&&n!==t;)li(n)&&rv(n[ie],n),n=n[Dt];n===null&&(n=t),li(n)&&rv(n[ie],n),e=n&&n[si]}n=e}}function T_(t,n){let e=t[ha],i=e.indexOf(n);e.splice(i,1)}function Jf(t,n){if(Co(n))return;let e=n[Le];e.destroyNode&&em(t,n,e,3,null,null),CN(n)}function rv(t,n){if(Co(n))return;let e=ce(null);try{n[ge]&=-129,n[ge]|=256,n[Wn]&&lo(n[Wn]),EN(t,n),wN(t,n),n[ie].type===1&&n[Le].destroy();let i=n[yo];if(i!==null&&qn(n[Dt])){i!==n[Dt]&&T_(i,n);let r=n[ur];r!==null&&r.detachView(t)}_v(n)}finally{ce(e)}}function fw(t,n,e,i){let r=t?.[bo];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Ca.add(t[Fr]),ux(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:u}=c();s.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),DN(t,i)}else t&&Ca.delete(t[Fr]),i(!1)},r)}function DN(t,n){let e=t[bo]?.running;if(e){e.then(()=>{t[bo].running=void 0,Ca.delete(t[Fr]),n(!0)});return}n(!1)}function wN(t,n){let e=t.cleanup,i=n[ds];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[ds]=null);let r=n[Pr];if(r!==null){n[Pr]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[vo];if(o!==null){n[vo]=null;for(let a of o)a.destroy()}}function EN(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof ba)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];Be(Oe.LifecycleHookStart,s,l);try{l.call(s)}finally{Be(Oe.LifecycleHookEnd,s,l)}}else{Be(Oe.LifecycleHookStart,r,o);try{o.call(r)}finally{Be(Oe.LifecycleHookEnd,r,o)}}}}}function mx(t,n,e){return xN(t,n.parent,e)}function xN(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[cn];if(mr(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Bi.None||r===Bi.Emulated)return null}return ci(i,e)}function px(t,n,e){return MN(t,n,e)}function SN(t,n,e){return t.type&40?ci(t,e):null}var MN=SN,mw;function A_(t,n,e,i){let r=mx(t,i,n),o=n[Le],a=i.parent||n[dn],s=px(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)lw(o,r,e[l],s,!1);else lw(o,r,e,s,!1);mw!==void 0&&mw(o,i,n,e,r)}function lc(t,n){if(n!==null){let e=n.type;if(e&3)return ci(n,t);if(e&4)return Nv(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return lc(t,i);{let r=t[n.index];return qn(r)?Nv(-1,r):Yn(r)}}else{if(e&128)return lc(t,n.next);if(e&32)return S_(n,t)()||Yn(t[n.index]);{let i=hx(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=_o(t[Ut]);return lc(r,i)}else return lc(t,n.next)}}}return null}function hx(t,n){if(n!==null){let i=t[Ut][dn],r=n.projection;return i.projection[r]}return null}function Nv(t,n){let e=pt+t+1;if(e<n.length){let i=n[e],r=i[ie].firstChild;if(r!==null)return lc(i,r)}return n[fr]}function R_(t,n,e,i,r,o,a){for(;e!=null;){let s=i[Gn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&ws(Yn(l),i),e.flags|=2),!Dc(e))if(c&8)R_(t,n,e.child,i,r,o,!1),_s(n,t,s,r,l,e,o,i);else if(c&32){let u=S_(e,i),m;for(;m=u();)_s(n,t,s,r,m,e,o,i);_s(n,t,s,r,l,e,o,i)}else c&16?gx(t,n,i,e,r,o):_s(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function em(t,n,e,i,r,o){R_(e,i,t.firstChild,n,r,o,!1)}function IN(t,n,e){let i=n[Le],r=mx(t,e,n),o=e.parent||n[dn],a=px(o,e,n);gx(i,0,n,e,r,a)}function gx(t,n,e,i,r,o){let a=e[Ut],l=a[dn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];_s(n,t,e[Gn],r,u,i,o,e)}else{let c=l,u=a[Dt];Rf(i)&&(c.flags|=128),R_(t,n,c,u,r,o,!0)}}function TN(t,n,e,i,r,o,a){let s=i[fr],l=Yn(i);s!==l&&_s(n,t,e,o,s,r,a);for(let c=pt;c<i.length;c++){let u=i[c];em(u[ie],u,t,n,o,s)}}function AN(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:_r.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=_r.Important),t.setStyle(e,i,r,o))}}function vx(t,n,e,i,r){let o=pr(),a=i&2;try{wo(-1),a&&n.length>Ne&&cx(t,n,Ne,!1);let s=a?Oe.TemplateUpdateStart:Oe.TemplateCreateStart;Be(s,r,e),e(i,r)}finally{wo(o);let s=a?Oe.TemplateUpdateEnd:Oe.TemplateCreateEnd;Be(s,r,e)}}function tm(t,n,e){FN(t,n,e),(e.flags&64)===64&&LN(t,n,e)}function xc(t,n,e=ci){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function RN(t,n,e,i){let o=i.get(a_,CE)||e===Bi.ShadowDom||e===Bi.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);if(a.tagName.toLowerCase()==="script")throw new P(905,!1);return kN(a),a}function kN(t){_x(t)}var _x=()=>null;function ON(t){oE(t)?XE(t):bO(t)}function yx(){_x=ON}function NN(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function bx(t,n,e,i,r,o){let a=n[ie];if(F_(t,a,n,e,i)){mr(t)&&PN(n,t.index);return}t.type&3&&(e=NN(e)),Cx(t,n,e,i,r,o)}function Cx(t,n,e,i,r,o){if(t.type&3){let a=ci(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function PN(t,n){let e=di(n,t);e[ge]&16||(e[ge]|=64)}function FN(t,n,e){let i=e.directiveStart,r=e.directiveEnd;mr(e)&&uN(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Af(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=dc(n,t,a,e);if(ws(l,n),o!==null&&HN(n,a-i,l,s,e,o),Fi(s)){let c=di(e.index,n);c[wt]=dc(n,t,a,e)}}}function LN(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=PD();try{wo(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];af(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&VN(l,c)}}finally{wo(-1),af(a)}}function VN(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function k_(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];ax(n,o.selectors,!1)&&(i??=[],Fi(o)?i.unshift(o):i.push(o))}return i}function jN(t,n,e,i,r,o){let a=ci(t,n);BN(n[Le],a,o,t.value,e,i,r)}function BN(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?ss(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function HN(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];kv(i,e,l,c)}}function O_(t,n,e,i,r){let o=Ne+e,a=n[ie],s=r(a,n,t,i,e);n[o]=s,ps(t,!0);let l=t.type===2;return l?(JE(n[Le],s,t),(SD()===0||fs(t))&&ws(s,n),MD()):ws(s,n),uf()&&(!l||!Dc(t))&&A_(a,n,s,t),t}function N_(t){let n=t;return $g()?Gg():(n=n.parent,ps(n,!1)),n}function P_(t,n){let e=t[Gn];if(!e)return;let i;try{i=e.get(Zn,null)}catch{i=null}i?.(n)}function F_(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],u=a[l+1],m=n.data[c];kv(m,e[c],u,r),s=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];kv(u,c,i,r),s=!0}return s}function UN(t,n){let e=di(n,t),i=e[ie];zN(i,e);let r=e[cn];r!==null&&e[un]===null&&(e[un]=FE(r,e[Gn])),Be(Oe.ComponentStart);try{L_(i,e,e[wt])}finally{Be(Oe.ComponentEnd,e[wt])}}function zN(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function L_(t,n,e){lf(n);try{let i=t.viewQuery;i!==null&&wv(1,i,e);let r=t.template;r!==null&&vx(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[ur]?.finishViewCreation(t),t.staticContentQueries&&UE(t,n),t.staticViewQueries&&wv(2,t.viewQuery,e);let o=t.components;o!==null&&$N(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[ge]&=-5,cf()}}function $N(t,n){for(let e=0;e<n.length;e++)UN(t,n[e])}function Ts(t,n,e,i){let r=ce(null);try{let o=n.tView,s=t[ge]&4096?4096:16,l=E_(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[yo]=c;let u=t[ur];return u!==null&&(l[ur]=u.createEmbeddedView(o)),L_(o,l,e),l}finally{ce(r)}}function Da(t,n){return!n||n.firstChild===null||Rf(t)}function mc(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Yn(o)),qn(o)&&Dx(o,i);let a=e.type;if(a&8)mc(t,n,e.child,i);else if(a&32){let s=S_(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=hx(n,e);if(Array.isArray(s))i.push(...s);else{let l=_o(n[Ut]);mc(l[ie],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function Dx(t,n){for(let e=pt;e<t.length;e++){let i=t[e],r=i[ie].firstChild;r!==null&&mc(i[ie],i,r,n)}t[fr]!==t[cn]&&n.push(t[fr])}function wx(t){if(t[pa]!==null){for(let n of t[pa])n.impl.addSequence(n);t[pa].length=0}}var Ex=[];function GN(t){return t[Wn]??WN(t)}function WN(t){let n=Ex.pop()??Object.create(YN);return n.lView=t,n}function qN(t){t.lView[Wn]!==t&&(t.lView=null,Ex.push(t))}var YN=fe(D({},oo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{va(t.lView)},consumerOnSignalRead(){this.lView[Wn]=this}});function QN(t){let n=t[Wn]??Object.create(ZN);return n.lView=t,n}var ZN=fe(D({},oo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=_o(t.lView);for(;n&&!xx(n[ie]);)n=_o(n);n&&Fg(n)},consumerOnSignalRead(){this.lView[Wn]=this}});function xx(t){return t.type!==2}function Sx(t){if(t[vo]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[vo])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[ge]&8192)}}var KN=100;function Mx(t,n=0){let i=t[Ni].rendererFactory,r=!1;r||i.begin?.();try{XN(t,n)}finally{r||i.end?.()}}function XN(t,n){let e=Wg();try{ql(!0),Pv(t,n);let i=0;for(;nc(t);){if(i===KN)throw new P(103,!1);i++,Pv(t,1)}}finally{ql(e)}}function JN(t,n,e,i){if(Co(n))return;let r=n[ge],o=!1,a=!1;lf(n);let s=!0,l=null,c=null;o||(xx(t)?(c=GN(n),l=kr(c)):tu()===null?(s=!1,c=QN(n),l=kr(c)):n[Wn]&&(lo(n[Wn]),n[Wn]=null));try{Pg(n),kD(t.bindingStartIndex),e!==null&&vx(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let _=t.preOrderCheckHooks;_!==null&&yf(n,_,null)}else{let _=t.preOrderHooks;_!==null&&bf(n,_,0,null),tv(n,0)}if(a||eP(n),Sx(n),Ix(n,0),t.contentQueries!==null&&UE(t,n),!o)if(u){let _=t.contentCheckHooks;_!==null&&yf(n,_)}else{let _=t.contentHooks;_!==null&&bf(n,_,1),tv(n,1)}nP(t,n);let m=t.components;m!==null&&Ax(n,m,0);let v=t.viewQuery;if(v!==null&&wv(2,v,i),!o)if(u){let _=t.viewCheckHooks;_!==null&&yf(n,_)}else{let _=t.viewHooks;_!==null&&bf(n,_,2),tv(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Xu]){for(let _ of n[Xu])_();n[Xu]=null}o||(wx(n),n[ge]&=-73)}catch(u){throw o||va(n),u}finally{c!==null&&(so(c,l),s&&qN(c)),cf()}}function Ix(t,n){for(let e=lE(t);e!==null;e=cE(e))for(let i=pt;i<e.length;i++){let r=e[i];Tx(r,n)}}function eP(t){for(let n=lE(t);n!==null;n=cE(n)){if(!(n[ge]&2))continue;let e=n[ha];for(let i=0;i<e.length;i++){let r=e[i];Fg(r)}}}function tP(t,n,e){Be(Oe.ComponentStart);let i=di(n,t);try{Tx(i,e)}finally{Be(Oe.ComponentEnd,i[wt])}}function Tx(t,n){ef(t)&&Pv(t,n)}function Pv(t,n){let i=t[ie],r=t[ge],o=t[Wn],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&Za(o)),a||=!1,o&&(o.dirty=!1),t[ge]&=-9217,a)JN(i,t,i.template,t[wt]);else if(r&8192){let s=ce(null);try{Sx(t),Ix(t,1);let l=i.components;l!==null&&Ax(t,l,1),wx(t)}finally{ce(s)}}}function Ax(t,n,e){for(let i=0;i<n.length;i++)tP(t,n[i],e)}function nP(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)wo(~r);else{let o=r,a=e[++i],s=e[++i];ND(a,o);let l=n[o];Be(Oe.HostBindingsUpdateStart,l);try{s(2,l)}finally{Be(Oe.HostBindingsUpdateEnd,l)}}}}finally{wo(-1)}}function nm(t,n){let e=Wg()?64:1088;for(t[Ni].changeDetectionScheduler?.notify(n);t;){t[ge]|=e;let i=_o(t);if(ga(t)&&!i)return t;t=i}return null}function Rx(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function kx(t,n){let e=pt+n;if(e<t.length)return t[e]}function As(t,n,e,i=!0){let r=n[ie];if(iP(r,n,t,e),i){let a=Nv(e,t),s=n[Le],l=s.parentNode(t[fr]);l!==null&&bN(r,t[dn],s,n,l,a)}let o=n[un];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function V_(t,n){let e=pc(t,n);return e!==void 0&&Jf(e[ie],e),e}function pc(t,n){if(t.length<=pt)return;let e=pt+n,i=t[e];if(i){let r=i[yo];r!==null&&r!==t&&T_(r,i),n>0&&(t[e-1][si]=i[si]);let o=Jl(t,pt+n);yN(i[ie],i);let a=o[ur];a!==null&&a.detachView(o[ie]),i[Dt]=null,i[si]=null,i[ge]&=-129}return i}function iP(t,n,e,i){let r=pt+i,o=e.length;i>0&&(e[r-1][si]=n),i<o-pt?(n[si]=e[r],Eg(e,pt+i,n)):(e.push(n),n[si]=null),n[Dt]=e;let a=n[yo];a!==null&&e!==a&&Ox(a,n);let s=n[ur];s!==null&&s.insertView(t),tf(n),n[ge]|=128}function Ox(t,n){let e=t[ha],i=n[Dt];if(li(i))t[ge]|=2;else{let r=i[Dt][Ut];n[Ut]!==r&&(t[ge]|=2)}e===null?t[ha]=[n]:e.push(n)}var Eo=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ie];return mc(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[wt]}set context(n){this._lView[wt]=n}get destroyed(){return Co(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Dt];if(qn(n)){let e=n[tc],i=e?e.indexOf(this):-1;i>-1&&(pc(n,i),Jl(e,i))}this._attachedToViewContainer=!1}Jf(this._lView[ie],this._lView)}onDestroy(n){Lg(this._lView,n)}markForCheck(){nm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[ge]&=-129}reattach(){tf(this._lView),this._lView[ge]|=128}detectChanges(){this._lView[ge]|=1024,Mx(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new P(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=ga(this._lView),e=this._lView[yo];e!==null&&!n&&T_(e,this._lView),fx(this._lView[ie],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new P(902,!1);this._appRef=n;let e=ga(this._lView),i=this._lView[yo];i!==null&&!e&&Ox(i,this._lView),tf(this._lView)}};var Ve=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=rP;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Ts(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Eo(o)}}return t})();function rP(){return im(Pt(),le())}function im(t,n){return t.type&4?new Ve(n,t,Ss(t,n)):null}function Rs(t,n,e,i,r){let o=t.data[n];if(o===null)o=oP(t,n,e,i,r),OD()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=TD();o.injectorIndex=a===null?-1:a.injectorIndex}return ps(o,!0),o}function oP(t,n,e,i,r){let o=zg(),a=$g(),s=a?o:o&&o.parent,l=t.data[n]=sP(t,s,e,n,i,r);return aP(t,l,o,a),l}function aP(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function sP(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return rf()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:df(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var lP=new RegExp(`^(\\d+)*(${mE}|${fE})*(.*)`);function cP(t){let n=t.match(lP),[e,i,r,o]=n,a=i?parseInt(i,10):r,s=[];for(let[l,c,u]of o.matchAll(/(f|n)(\d*)/g)){let m=parseInt(u,10)||1;s.push(c,m)}return[a,...s]}function dP(t){return!t.prev&&t.parent?.type===8}function ov(t){return t.index-Ne}function uP(t,n){let e=t.i18nNodes;if(e)return e.get(n)}function rm(t,n,e,i){let r=ov(i),o=uP(t,r);if(o===void 0){let a=t.data[vE];if(a?.[r])o=mP(a[r],e);else if(n.firstChild===i)o=t.firstChild;else{let s=i.prev===null,l=i.prev??i.parent;if(dP(i)){let c=ov(i.parent);o=Dv(t,c)}else{let c=ci(l,e);if(s)o=c.firstChild;else{let u=ov(l),m=Dv(t,u);if(l.type===2&&m){let _=u_(t,u)+1;o=om(_,m)}else o=c.nextSibling}}}}return o}function om(t,n){let e=n;for(let i=0;i<t;i++)e=e.nextSibling;return e}function fP(t,n){let e=t;for(let i=0;i<n.length;i+=2){let r=n[i],o=n[i+1];for(let a=0;a<o;a++)switch(r){case fO:e=e.firstChild;break;case mO:e=e.nextSibling;break}}return e}function mP(t,n){let[e,...i]=cP(t),r;if(e===fE)r=n[Ut][cn];else if(e===mE)r=ix(n[Ut][cn]);else{let o=Number(e);r=Yn(n[o+Ne])}return fP(r,i)}var pP=!1;function Nx(t){pP=t}function hP(t){let n=t[un];if(n){let{i18nNodes:e,dehydratedIcuData:i}=n;if(e&&i){let r=t[Le];for(let o of i.values())gP(r,e,o)}n.i18nNodes=void 0,n.dehydratedIcuData=void 0}}function gP(t,n,e){for(let i of e.node.cases[e.case]){let r=n.get(i.index-Ne);r&&b_(t,r,!1)}}function am(t){let n=t[Pi]??[],i=t[Dt][Le],r=[];for(let o of n)o.data[_E]!==void 0?r.push(o):Px(o,i);t[Pi]=r}function vP(t){let{lContainer:n}=t,e=n[Pi];if(e===null)return;let r=n[Dt][Le];for(let o of e)Px(o,r)}function Px(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[uc];for(;e<r;){let o=i.nextSibling;b_(n,i,!1),i=o,e++}}}function sm(t){am(t);let n=t[cn];li(n)&&Of(n);for(let e=pt;e<t.length;e++)Of(t[e])}function Of(t){hP(t);let n=t[ie];for(let e=Ne;e<n.bindingStartIndex;e++)if(qn(t[e])){let i=t[e];sm(i)}else li(t[e])&&Of(t[e])}function j_(t){let n=t._views;for(let e of n){let i=LE(e);i!==null&&i[cn]!==null&&(li(i)?Of(i):sm(i))}}function _P(t,n,e,i){t!==null&&(e.cleanup(n),sm(t.lContainer),j_(i))}function yP(t,n){let e=[];for(let i of n)for(let r=0;r<(i[r_]??1);r++){let o={data:i,firstChild:null};i[uc]>0&&(o.firstChild=t,t=om(i[uc],t)),e.push(o)}return[t,e]}var Fx=()=>null,Lx=()=>null;function Vx(){Fx=bP,Lx=CP}function bP(t,n){return Bx(t,n)?t[Pi].shift():(am(t),null)}function hc(t,n){return Fx(t,n)}function CP(t,n,e){if(n.tView.ssrId===null)return null;let i=hc(t,n.tView.ssrId);return e[ie].firstUpdatePass&&i===null&&DP(e,n),i}function jx(t,n,e){return Lx(t,n,e)}function DP(t,n){let e=n;for(;e;){if(pw(t,e))return;if((e.flags&256)===256)break;e=e.prev}for(e=n.next;e&&(e.flags&512)===512;){if(pw(t,e))return;e=e.next}}function Bx(t,n){let e=t[Pi];return!n||e===null||e.length===0?!1:e[0].data[gE]===n}function pw(t,n){let e=n.tView?.ssrId;if(e==null)return!1;let i=t[n.index];return qn(i)&&Bx(i,e)?(am(i),!0):!1}var Hx=class{},lm=class{},Fv=class{resolveComponentFactory(n){throw new P(917,!1)}},Sc=class{static NULL=new Fv},Tt=class{},qe=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>wP()}return t})();function wP(){let t=le(),n=Pt(),e=di(n.index,t);return(li(e)?e:t)[Le]}var Ux=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>null})}return t})();var wf={},bs=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,wf,i);return r!==wf||e===wf?r:this.parentInjector.get(n,e,i)}};function Nf(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=$u(r,s);else if(o==2){let l=s,c=n[++a];i=$u(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ee(t,n=0){let e=le();if(e===null)return se(t,n);let i=Pt();return Jw(i,e,Nt(t),n)}function cm(){let t="invalid";throw new Error(t)}function zx(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let u of a)if(u.resolveHostDirectives!==null){[s,l,c]=u.resolveHostDirectives(a);break}SP(t,n,e,s,o,l,c)}o!==null&&i!==null&&EP(e,i,o)}function EP(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new P(-301,!1);i.push(n[r],o)}}function xP(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function SP(t,n,e,i,r,o,a){let s=i.length,l=null;for(let v=0;v<s;v++){let _=i[v];l===null&&Fi(_)&&(l=_,xP(t,e,v)),gv(Af(e,n),t,_.type)}kP(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let v=0;v<s;v++){let _=i[v];_.providersResolver&&_.providersResolver(_)}let c=!1,u=!1,m=lx(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let v=0;v<s;v++){let _=i[v];if(e.mergedAttrs=Ds(e.mergedAttrs,_.hostAttrs),IP(t,e,n,m,_),RP(m,_,r),a!==null&&a.has(_)){let[I,R]=a.get(_);e.directiveToIndex.set(_.type,[m,I+e.directiveStart,R+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,m);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let y=_.type.prototype;!c&&(y.ngOnChanges||y.ngOnInit||y.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(y.ngOnChanges||y.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),m++}MP(t,e,o)}function MP(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))hw(0,n,r,i),hw(1,n,r,i),vw(n,i,!1);else{let o=e.get(r);gw(0,n,o,i),gw(1,n,o,i),vw(n,i,!0)}}}function hw(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),$x(n,o)}}function gw(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),$x(n,a)}}function $x(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function vw(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||D_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){a??=[],a.push(c[u+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function IP(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=go(r.type,!0)),a=new ba(o,Fi(r),ee,null);t.blueprint[i]=a,e[i]=a,TP(t,n,i,lx(t,e,r.hostVars,mn),r)}function TP(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;AP(a)!=s&&a.push(s),a.push(e,i,o)}}function AP(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function RP(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Fi(n)&&(e[""]=t)}}function kP(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function B_(t,n,e,i,r,o,a,s){let l=n[ie],c=l.consts,u=Qn(c,a),m=Rs(l,t,e,i,u);return o&&zx(l,n,m,Qn(c,s),r),m.mergedAttrs=Ds(m.mergedAttrs,m.attrs),m.attrs!==null&&Nf(m,m.attrs,!1),m.mergedAttrs!==null&&Nf(m,m.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,m),m}function H_(t,n){$w(t,n),Rg(n)&&t.queries.elementEnd(n)}function OP(t,n,e,i,r,o){let a=n.consts,s=Qn(a,r),l=Rs(n,t,e,i,s);if(l.mergedAttrs=Ds(l.mergedAttrs,l.attrs),o!=null){let c=Qn(a,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Nf(l,l.attrs,!1),l.mergedAttrs!==null&&Nf(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function U_(t){return Wx(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function Gx(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function Wx(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function dm(t,n,e){return t[n]=e}function NP(t,n){return t[n]}function Xn(t,n,e){if(e===mn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function z_(t,n,e,i){let r=Xn(t,n,e);return Xn(t,n+1,i)||r}function PP(t,n,e,i,r){let o=z_(t,n,e,i);return Xn(t,n+2,r)||o}function Ef(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&pO(r,o);let a=mr(t)?di(t.index,n):n;nm(a,5);let s=n[wt],l=_w(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=_w(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function _w(t,n,e,i){let r=ce(null);try{return Be(Oe.OutputStart,n,e),e(i)!==!1}catch(o){return P_(t,o),!1}finally{Be(Oe.OutputEnd,n,e),ce(r)}}function qx(t,n,e,i,r,o,a,s){let l=fs(t),c=!1,u=null;if(!i&&l&&(u=LP(n,e,o,t.index)),u!==null){let m=u.__ngLastListenerFn__||u;m.__ngNextListenerFn__=a,u.__ngLastListenerFn__=a,c=!0}else{let m=ci(t,e),v=i?i(m):m;hO(e,v,o,s),i||(s.__ngNativeEl__=m);let _=r.listen(v,o,s);if(!FP(o)){let y=i?I=>i(Yn(I[t.index])):t.index;Yx(y,n,e,o,s,_,!1)}}return c}function FP(t){return t.startsWith("animation")||t.startsWith("transition")}function LP(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[ds],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function Yx(t,n,e,i,r,o,a){let s=n.firstCreatePass?jg(n):null,l=Vg(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function yw(t,n,e,i,r,o){let a=n[e],s=n[ie],c=s.data[e].outputs[i],m=a[c].subscribe(o);Yx(t.index,s,n,r,o,m,!0)}var Lv=Symbol("BINDING");function Qx(t){return t.debugInfo?.className||t.type.name||null}var Pf=class extends Sc{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=ki(n);return new xo(e,this.ngModule)}};function VP(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Zf.SignalBased)!==0};return r&&(o.transform=r),o})}function jP(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function BP(t,n,e){let i=n instanceof Ge?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new bs(e,i):e}function HP(t){let n=t.get(Tt,null);if(n===null)throw new P(407,!1);let e=t.get(Ux,null),i=t.get(Ri,null),r=t.get(Ui,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function UP(t,n){let e=Zx(t);return y_(n,e,e==="svg"?kg:e==="math"?bD:null)}function Zx(t){return(t.selectors[0][0]||"div").toLowerCase()}var xo=class extends lm{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=VP(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=jP(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=sN(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){Be(Oe.DynamicComponentStart);let s=ce(null);try{let l=this.componentDef,c=BP(l,r||this.ngModule,n),u=HP(c),m=u.tracingService;return m&&m.componentCreate?m.componentCreate(Qx(l),()=>this.createComponentRef(u,c,e,i,o,a)):this.createComponentRef(u,c,e,i,o,a)}finally{ce(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=zP(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),u=r?RN(c,r,s.encapsulation,e):UP(s,c),m=a?.some(bw)||o?.some(y=>typeof y!="function"&&y.bindings.some(bw)),v=E_(null,l,null,512|sx(s),null,null,n,c,e,null,FE(u,e,!0));v[Ne]=u,lf(v);let _=null;try{let y=B_(Ne,v,2,"#host",()=>l.directiveRegistry,!0,0);JE(c,u,y),ws(u,v),tm(l,v,y),f_(l,y,v),H_(l,y),i!==void 0&&GP(y,this.ngContentSelectors,i),_=di(y.index,v),v[wt]=_[wt],L_(l,v,null)}catch(y){throw _!==null&&_v(_),_v(v),y}finally{Be(Oe.DynamicComponentEnd),cf()}return new Ff(this.componentType,v,!!m)}};function zP(t,n,e,i){let r=t?["ng-version","21.2.15"]:lN(n.selectors[0]),o=null,a=null,s=0;if(e)for(let u of e)s+=u[Lv].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(a??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let m=i[u];if(typeof m!="function")for(let v of m.bindings){s+=v[Lv].requiredVars;let _=u+1;v.create&&(v.targetIdx=_,(o??=[]).push(v)),v.update&&(v.targetIdx=_,(a??=[]).push(v))}}let l=[n];if(i)for(let u of i){let m=typeof u=="function"?u:u.type,v=Xl(m);l.push(v)}return w_(0,null,$P(o,a),1,s,l,null,null,null,[r],null)}function $P(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function bw(t){let n=t[Lv].kind;return n==="input"||n==="twoWay"}var Ff=class extends Hx{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=ms(e[ie],Ne),this.location=Ss(this._tNode,e),this.instance=di(this._tNode.index,e)[wt],this.hostView=this.changeDetectorRef=new Eo(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=F_(i,r[ie],r,n,e);this.previousInputValues.set(n,e);let a=di(i.index,r);nm(a,1)}get injector(){return new ya(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function GP(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var ct=(()=>{class t{static __NG_ELEMENT_ID__=WP}return t})();function WP(){let t=Pt();return Kx(t,le())}var Vv=class t extends ct{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Ss(this._hostTNode,this._hostLView)}get injector(){return new ya(this._hostTNode,this._hostLView)}get parentInjector(){let n=t_(this._hostTNode,this._hostLView);if(qw(n)){let e=If(n,this._hostLView),i=Mf(n),r=e[ie].data[i+8];return new ya(r,e)}else return new ya(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Cw(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-pt}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=hc(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,Da(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!j1(n),c;if(l)c=e;else{let R=e||{};c=R.index,i=R.injector,r=R.projectableNodes,o=R.environmentInjector||R.ngModuleRef,a=R.directives,s=R.bindings}let u=l?n:new xo(ki(n)),m=i||this.parentInjector;if(!o&&u.ngModule==null){let V=(l?m:this.parentInjector).get(Ge,null);V&&(o=V)}let v=ki(u.componentType??{}),_=hc(this._lContainer,v?.id??null),y=_?.firstChild??null,I=u.create(m,r,y,o,a,s);return this.insertImpl(I.hostView,c,Da(this._hostTNode,_)),I}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(DD(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[Dt],c=new t(l,l[dn],l[Dt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return As(a,r,o,i),n.attachToViewContainerRef(),Eg(av(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Cw(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=pc(this._lContainer,e);i&&(Jl(av(this._lContainer),e),Jf(i[ie],i))}detach(n){let e=this._adjustIndex(n,-1),i=pc(this._lContainer,e);return i&&Jl(av(this._lContainer),e)!=null?new Eo(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Cw(t){return t[tc]}function av(t){return t[tc]||(t[tc]=[])}function Kx(t,n){let e,i=n[t.index];return qn(i)?e=i:(e=Rx(i,n,null,t),n[t.index]=e,x_(n,e)),Xx(e,n,t,i),new Vv(e,t,n)}function qP(t,n){let e=t[Le],i=e.createComment(""),r=ci(n,t),o=e.parentNode(r);return kf(e,o,i,e.nextSibling(r),!1),i}var Xx=Jx,$_=()=>!1;function YP(t,n,e){return $_(t,n,e)}function Jx(t,n,e,i){if(t[fr])return;let r;e.type&8?r=Yn(i):r=qP(n,e),t[fr]=r}function QP(t,n,e){if(t[fr]&&t[Pi])return!0;let i=e[un],r=n.index-Ne;if(!i||aE(n)||BE(i,r))return!1;let a=Dv(i,r),s=i.data[Gf]?.[r];if(s===void 0)return!1;let[l,c]=yP(a,s);return t[fr]=l,t[Pi]=c,!0}function ZP(t,n,e,i){$_(t,e,n)||Jx(t,n,e,i)}function eS(){Xx=ZP,$_=QP}var jv=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Bv=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)W_(n,e).matches!==null&&this.queries[e].setDirty()}},Lf=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=tF(n):this.predicate=n}},Hv=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Uv=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,KP(e,o)),this.matchTNodeWithReadOption(n,e,Cf(e,n,o,!1,!1))}else i===Ve?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Cf(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===W||r===ct||r===Ve&&e.type&4)this.addMatch(e.index,-2);else{let o=Cf(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function KP(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function XP(t,n){return t.type&11?Ss(t,n):t.type&4?im(t,n):null}function JP(t,n,e,i){return e===-1?XP(n,t):e===-2?eF(t,n,i):dc(t,t[ie],e,n)}function eF(t,n,e){if(e===W)return Ss(n,t);if(e===Ve)return im(n,t);if(e===ct)return Kx(n,t)}function tS(t,n,e,i){let r=n[ur].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let u=o[c];s.push(JP(n,u,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function zv(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=tS(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],u=n[-l];for(let m=pt;m<u.length;m++){let v=u[m];v[yo]===v[Dt]&&zv(v[ie],v,c,i)}if(u[ha]!==null){let m=u[ha];for(let v=0;v<m.length;v++){let _=m[v];zv(_[ie],_,c,i)}}}}}return i}function G_(t,n){return t[ur].queries[n].queryList}function nS(t,n,e){let i=new Xt((e&4)===4);return xD(t,n,i,i.destroy),(n[ur]??=new Bv).queries.push(new jv(i))-1}function iS(t,n,e){let i=nt();return i.firstCreatePass&&(oS(i,new Lf(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),nS(i,le(),n)}function rS(t,n,e,i){let r=nt();if(r.firstCreatePass){let o=Pt();oS(r,new Lf(n,e,i),o.index),nF(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return nS(r,le(),e)}function tF(t){return t.split(",").map(n=>n.trim())}function oS(t,n,e){t.queries===null&&(t.queries=new Hv),t.queries.track(new Uv(n,e))}function nF(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function W_(t,n){return t.queries.getByIndex(n)}function aS(t,n){let e=t[ie],i=W_(e,n);return i.crossesNgTemplate?zv(e,t,n,[]):tS(e,t,i,n)}function sS(t,n,e){let i,r=Rl(()=>{i._dirtyCounter();let o=iF(i,t);if(n&&o===void 0)throw new P(-951,!1);return o});return i=r[bt],i._dirtyCounter=S(0),i._flatValue=void 0,r}function q_(t){return sS(!0,!1,t)}function Y_(t){return sS(!0,!0,t)}function lS(t,n){let e=t[bt];e._lView=le(),e._queryIndex=n,e._queryList=G_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function iF(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[ge]&4)return n?void 0:Kt;let r=G_(e,i),o=aS(e,i);return r.reset(o,nE),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var yr=class{},um=class{};var Vf=class extends yr{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Pf(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=bg(n);this._bootstrapComponents=rx(o.bootstrap),this._r3Injector=Qg(n,e,[{provide:yr,useValue:this},{provide:Sc,useValue:this.componentFactoryResolver},...i],Zl(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},jf=class extends um{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Vf(this.moduleType,n,[])}};var gc=class extends yr{injector;componentFactoryResolver=new Pf(this);instance=null;constructor(n){super();let e=new da([...n.providers,{provide:yr,useValue:this},{provide:Sc,useValue:this.componentFactoryResolver}],n.parent||cs(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ks(t,n,e=null){return new gc({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var rF=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Ku(!1,e.type),r=i.length>0?ks([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t(se(Ge))})}return t})();function O(t){return _c(()=>{let n=cS(t),e=fe(D({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===n_.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(rF).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Bi.Emulated,styles:t.styles||Kt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&fi("NgStandalone"),dS(e);let i=t.dependencies;return e.directiveDefs=Dw(i,oF),e.pipeDefs=Dw(i,Cg),e.id=lF(e),e})}function oF(t){return ki(t)||Xl(t)}function U(t){return _c(()=>({type:t.type,bootstrap:t.bootstrap||Kt,declarations:t.declarations||Kt,imports:t.imports||Kt,exports:t.exports||Kt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function aF(t,n){if(t==null)return Oi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=Zf.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function sF(t){if(t==null)return Oi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function M(t){return _c(()=>{let n=cS(t);return dS(n),n})}function fm(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function cS(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Oi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Kt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:aF(t.inputs,n),outputs:sF(t.outputs),debugInfo:null}}function dS(t){t.features?.forEach(n=>n(t))}function Dw(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function lF(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function Q_(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=cF,e.hostDirectives=i?t.map($v):[t]):i?e.hostDirectives.unshift(...t.map($v)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function cF(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,uS(a,n,i),r.set(a,[s,n.length-1])}o===0&&Fi(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function uS(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)ww($v(o),n,e)}else ww(i,n,e)}function ww(t,n,e){let i=Xl(t.directive);dF(i.declaredInputs,t.inputs),uS(i,n,e),e.set(i,t),n.push(i)}function $v(t){return typeof t=="function"?{directive:Nt(t),inputs:Oi,outputs:Oi}:{directive:Nt(t.directive),inputs:Ew(t.inputs),outputs:Ew(t.outputs)}}function Ew(t){if(t===void 0||t.length===0)return Oi;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function dF(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function uF(t){return Object.getPrototypeOf(t.prototype).constructor}function ae(t){let n=uF(t.type),e=!0,i=[t];for(;n;){let r;if(Fi(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new P(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let a=t;a.inputs=sv(t.inputs),a.declaredInputs=sv(t.declaredInputs),a.outputs=sv(t.outputs);let s=r.hostBindings;s&&gF(t,s);let l=r.viewQuery,c=r.contentQueries;if(l&&pF(t,l),c&&hF(t,c),fF(t,r),oD(t.outputs,r.outputs),Fi(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let a=0;a<o.length;a++){let s=o[a];s&&s.ngInherit&&s(t),s===ae&&(e=!1)}}n=Object.getPrototypeOf(n)}mF(i)}function fF(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function mF(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Ds(r.hostAttrs,e=Ds(e,r.hostAttrs))}}function sv(t){return t===Oi?{}:t===Kt?[]:t}function pF(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function hF(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function gF(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function fS(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=Ds(t.mergedAttrs,t.attrs);let u=t.tView=w_(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),ps(t,!1);let l=mS(e,n,t,i);uf()&&A_(e,n,l,t),ws(l,n);let c=Rx(l,n,l,t);n[i+Ne]=c,x_(n,c),YP(c,t,n)}function vF(t,n,e,i,r,o,a,s,l,c,u){let m=e+Ne,v;return n.firstCreatePass?(v=Rs(n,m,4,a||null,s||null),nf()&&zx(n,t,v,Qn(n.consts,c),k_),$w(n,v)):v=n.data[m],fS(v,t,n,e,i,r,o,l),fs(v)&&tm(n,t,v),c!=null&&xc(t,v,u),v}function Es(t,n,e,i,r,o,a,s,l,c,u){let m=e+Ne,v;if(n.firstCreatePass){if(v=Rs(n,m,4,a||null,s||null),c!=null){let _=Qn(n.consts,c);v.localNames=[];for(let y=0;y<_.length;y+=2)v.localNames.push(_[y],-1)}}else v=n.data[m];return fS(v,t,n,e,i,r,o,l),c!=null&&xc(t,v,u),v}function N(t,n,e,i,r,o,a,s){let l=le(),c=nt(),u=Qn(c.consts,o);return vF(l,c,t,n,e,i,r,u,void 0,a,s),N}function Os(t,n,e,i,r,o,a,s){let l=le(),c=nt(),u=Qn(c.consts,o);return Es(l,c,t,n,e,i,r,u,void 0,a,s),Os}var mS=pS;function pS(t,n,e,i){return Lr(!0),n[Le].createComment("")}function _F(t,n,e,i){let r=!Yf(n,e);Lr(r);let o=n[un]?.data[hE]?.[i]??null;if(o!==null&&e.tView!==null&&e.tView.ssrId===null&&(e.tView.ssrId=o),r)return pS(t,n);let a=n[un],s=rm(a,t,n,e);qf(a,i,s);let l=u_(a,i);return om(l,s)}function hS(){mS=_F}var Kn=(function(t){return t[t.NOT_STARTED=0]="NOT_STARTED",t[t.IN_PROGRESS=1]="IN_PROGRESS",t[t.COMPLETE=2]="COMPLETE",t[t.FAILED=3]="FAILED",t})(Kn||{}),xw=0,yF=1,It=(function(t){return t[t.Placeholder=0]="Placeholder",t[t.Loading=1]="Loading",t[t.Complete=2]="Complete",t[t.Error=3]="Error",t})(It||{});var bF=0,Mc=1;var CF=4,DF=5;var wF=7,Cs=8,EF=9,Z_=(function(t){return t[t.Manual=0]="Manual",t[t.Playthrough=1]="Playthrough",t})(Z_||{});function xf(t,n){let e=SF(t),i=n[e];if(i!==null){for(let r of i)r();n[e]=null}}function xF(t){xf(1,t),xf(0,t),xf(2,t)}function SF(t){let n=CF;return t===1?n=DF:t===2&&(n=EF),n}function gS(t){return t+1}function Ns(t,n){let e=t[ie],i=gS(n.index);return t[i]}function Ic(t,n){let e=gS(n.index);return t.data[e]}function MF(t,n,e){let i=n[ie],r=Ic(i,e);switch(t){case It.Complete:return r.primaryTmplIndex;case It.Loading:return r.loadingTmplIndex;case It.Error:return r.errorTmplIndex;case It.Placeholder:return r.placeholderTmplIndex;default:return null}}function Sw(t,n){return n===It.Placeholder?t.placeholderBlockConfig?.[xw]??null:n===It.Loading?t.loadingBlockConfig?.[xw]??null:null}function IF(t){return t.loadingBlockConfig?.[yF]??null}function Mw(t,n){if(!t||t.length===0)return n;let e=new Set(t);for(let i of n)e.add(i);return t.length===e.size?t:Array.from(e)}function TF(t,n){let e=n.primaryTmplIndex+Ne;return ms(t,e)}var AF=(()=>{class t{cachedInjectors=new Map;getOrCreateInjector(e,i,r,o){if(!this.cachedInjectors.has(e)){let a=r.length>0?ks(r,i,o):null;this.cachedInjectors.set(e,a)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t})}return t})();var vS=new C("");function lv(t,n,e){return t.get(AF).getOrCreateInjector(n,t,e,"")}function RF(t,n,e){if(t instanceof bs){let r=t.injector,o=t.parentInjector,a=lv(o,n,e);return new bs(r,a)}let i=t.get(Ge);if(i!==t){let r=lv(i,n,e);return new bs(t,r)}return lv(t,n,e)}function _a(t,n,e,i=!1){let r=e[Dt],o=r[ie];if(Co(r))return;let a=Ns(r,n),s=a[Mc],l=a[wF];if(!(l!==null&&t<l)&&Iw(s,t)&&Iw(a[bF]??-1,t)){let c=Ic(o,n),m=!i&&!0&&(IF(c)!==null||Sw(c,It.Loading)!==null||Sw(c,It.Placeholder))?NF:OF;try{m(t,a,e,n,r)}catch(v){P_(r,v)}}}function kF(t,n){let e=t[Pi]?.findIndex(r=>r.data[yE]===n[Mc])??-1;return{dehydratedView:e>-1?t[Pi][e]:null,dehydratedViewIx:e}}function OF(t,n,e,i,r){Be(Oe.DeferBlockStateStart);let o=MF(t,r,i);if(o!==null){n[Mc]=t;let a=r[ie],s=o+Ne,l=ms(a,s),c=0;V_(e,c);let u;if(t===It.Complete){let y=Ic(a,i),I=y.providers;I&&I.length>0&&(u=RF(r[Gn],y,I))}let{dehydratedView:m,dehydratedViewIx:v}=kF(e,n),_=Ts(r,l,null,{injector:u,dehydratedView:m});if(As(e,_,c,Da(l,m)),nm(_,2),v>-1&&e[Pi]?.splice(v,1),(t===It.Complete||t===It.Error)&&Array.isArray(n[Cs])){for(let y of n[Cs])y();n[Cs]=null}}Be(Oe.DeferBlockStateEnd)}function Iw(t,n){return t<n}function Tw(t,n,e){t.loadingPromise.then(()=>{t.loadingState===Kn.COMPLETE?_a(It.Complete,n,e):t.loadingState===Kn.FAILED&&_a(It.Error,n,e)})}var NF=null;var mm=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function br(t){return typeof t=="function"&&t[bt]!==void 0}function K_(t){return br(t)&&typeof t.set=="function"}var X_=new C("");function So(t){return!!t&&typeof t.then=="function"}function J_(t){return!!t&&typeof t.subscribe=="function"}var _S=new C("");var ey=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(_S,{optional:!0})??[];injector=d(oe);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Bt(this.injector,r);if(So(o))e.push(o);else if(J_(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Mo=new C("");function yS(){Ph(()=>{let t="";throw new P(600,t)})}function bS(t){return t.isBoundToModule}var PF=10;var At=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Zn);afterRenderManager=d(Xf);zonelessEnabled=d(oc);rootEffectScheduler=d(mf);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new k;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(gr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(he(e=>!e))}constructor(){d(Ui,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Ge);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=oe.NULL){return this._injector.get(re).run(()=>{Be(Oe.BootstrapComponentStart);let a=e instanceof lm;if(!this._injector.get(ey).done){let y="";throw new P(405,y)}let l;a?l=e:l=this._injector.get(Sc).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=bS(l)?void 0:this._injector.get(yr),u=i||l.selector,m=l.create(r,[],u,c),v=m.location.nativeElement,_=m.injector.get(X_,null);return _?.registerApplication(v),m.onDestroy(()=>{this.detachView(m.hostView),cc(this.components,m),_?.unregisterApplication(v)}),this._loadComponent(m),Be(Oe.BootstrapComponentEnd,m),m})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Be(Oe.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Kf.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Be(Oe.ChangeDetectionEnd),new P(101,!1);let e=ce(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ce(e),this.afterTick.next(),Be(Oe.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Tt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<PF;){Be(Oe.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Be(Oe.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!nc(r))continue;let o=i&&!this.zonelessEnabled?0:1;Mx(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>nc(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;cc(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Mo,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>cc(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new P(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function cc(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ty(){let t,n;return{promise:new Promise((i,r)=>{t=i,n=r}),resolve:t,reject:n}}function CS(t,n,e){let i=n[Gn],r=n[ie];if(t.loadingState!==Kn.NOT_STARTED)return t.loadingPromise??Promise.resolve();let o=Ns(n,e),a=TF(r,t);t.loadingState=Kn.IN_PROGRESS,xf(1,o);let s=t.dependencyResolverFn,l=i.get(vs).add();return s?(t.loadingPromise=Promise.allSettled(s()).then(c=>{let u=!1,m=null,v=[],_=[];for(let y=0;y<c.length;y++){let I=c[y];if(I.status==="fulfilled"){let R=I.value,V=ki(R)||Xl(R);if(V)v.push(V);else{let _e=Cg(R);_e&&_.push(_e)}}else{u=!0,m=I.reason instanceof Error?I.reason:new Error(String(I.reason));break}}if(u){if(t.loadingState=Kn.FAILED,t.errorTmplIndex===null){let I="",R=new P(-750,I);P_(n,R)}}else{t.loadingState=Kn.COMPLETE;let y=a.tView;if(v.length>0){y.directiveRegistry=Mw(y.directiveRegistry,v);let I=v.map(V=>V.type),R=Ku(!1,...I);t.providers=R}_.length>0&&(y.pipeRegistry=Mw(y.pipeRegistry,_))}}),t.loadingPromise.finally(()=>{t.loadingPromise=null,l()})):(t.loadingPromise=Promise.resolve().then(()=>{t.loadingPromise=null,t.loadingState=Kn.COMPLETE,l()}),t.loadingPromise)}function FF(t,n){return n[Gn].get(vS,null,{optional:!0})?.behavior!==Z_.Manual}function LF(t,n,e){let i=n[ie],r=n[e.index];if(!FF(t,n))return;let o=Ns(n,e),a=Ic(i,e);switch(xF(o),a.loadingState){case Kn.NOT_STARTED:_a(It.Loading,e,r),CS(a,n,e),a.loadingState===Kn.IN_PROGRESS&&Tw(a,e,r);break;case Kn.IN_PROGRESS:_a(It.Loading,e,r),Tw(a,e,r);break;case Kn.COMPLETE:_a(It.Complete,e,r);break;case Kn.FAILED:_a(It.Error,e,r);break;default:}}async function DS(t,n,e){let i=t.get(Wf);if(i.hydrating.has(n))return;let{parentBlockPromise:o,hydrationQueue:a}=wO(n,t);if(a.length===0)return;o!==null&&a.shift(),BF(i,a),o!==null&&await o;let s=a[0];i.has(s)?await Aw(t,a,e):i.awaitParentBlock(s,async()=>await Aw(t,a,e))}async function Aw(t,n,e){let i=t.get(Wf),r=i.hydrating,o=t.get(gr),a=o.add();for(let l=0;l<n.length;l++){let c=n[l],u=i.get(c);if(u!=null){if(await UF(u),await HF(t),VF(u)){vP(u),Rw(n.slice(l),i);break}r.get(c).resolve()}else{jF(l,n,i),Rw(n.slice(l),i);break}}let s=n[n.length-1];await r.get(s)?.promise,o.remove(a),e&&e(n),_P(i.get(s),n,i,t.get(At))}function VF(t){return Ns(t.lView,t.tNode)[Mc]===It.Error}function jF(t,n,e){let i=t-1,r=i>-1?e.get(n[i]):null;r&&sm(r.lContainer)}function Rw(t,n){let e=n.hydrating;for(let i in t)e.get(i)?.reject();n.cleanup(t)}function BF(t,n){for(let e of n)t.hydrating.set(e,ty())}function HF(t){return new Promise(n=>ht(n,{injector:t}))}async function UF(t){let{tNode:n,lView:e}=t,i=Ns(e,n);return new Promise(r=>{zF(i,r),LF(2,e,n)})}function zF(t,n){Array.isArray(t[Cs])||(t[Cs]=[]),t[Cs].push(n)}function de(t,n,e,i){let r=le(),o=Do();if(Xn(r,o,n)){let a=nt(),s=rc();jN(s,r,t,n,e,i)}return de}var Gv=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function cv(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function $F(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){ce(i);let c=n.length-1;for(ce(null);a<=s&&a<=c;){let u=t.at(a),m=n[a],v=cv(a,u,a,m,e);if(v!==0){v<0&&t.updateValue(a,m),a++;continue}let _=t.at(s),y=n[c],I=cv(s,_,c,y,e);if(I!==0){I<0&&t.updateValue(s,y),s--,c--;continue}let R=e(a,u),V=e(s,_),_e=e(a,m);if(Object.is(_e,V)){let yt=e(c,y);Object.is(yt,R)?(t.swap(a,s),t.updateValue(s,y),c--,s--):t.move(s,a),t.updateValue(a,m),a++;continue}if(r??=new Bf,o??=Ow(t,a,s,e),Wv(t,r,a,_e))t.updateValue(a,m),a++,s++;else if(o.has(_e))r.set(R,t.detach(a)),s--;else{let yt=t.create(a,n[a]);t.attach(a,yt),a++,s++}}for(;a<=c;)kw(t,r,e,a,n[a]),a++}else if(n!=null){ce(i);let c=n[Symbol.iterator]();ce(null);let u=c.next();for(;!u.done&&a<=s;){let m=t.at(a),v=u.value,_=cv(a,m,a,v,e);if(_!==0)_<0&&t.updateValue(a,v),a++,u=c.next();else{r??=new Bf,o??=Ow(t,a,s,e);let y=e(a,v);if(Wv(t,r,a,y))t.updateValue(a,v),a++,s++,u=c.next();else if(!o.has(y))t.attach(a,t.create(a,v)),a++,s++,u=c.next();else{let I=e(a,m);r.set(I,t.detach(a)),s--}}}for(;!u.done;)kw(t,r,e,t.length,u.value),u=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function Wv(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function kw(t,n,e,i,r){if(Wv(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Ow(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Bf=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function T(t,n,e,i,r,o,a,s){fi("NgControlFlow");let l=le(),c=nt(),u=Qn(c.consts,o);return Es(l,c,t,n,e,i,r,u,256,a,s),ny}function ny(t,n,e,i,r,o,a,s){fi("NgControlFlow");let l=le(),c=nt(),u=Qn(c.consts,o);return Es(l,c,t,n,e,i,r,u,512,a,s),ny}function A(t,n){fi("NgControlFlow");let e=le(),i=Do(),r=e[i]!==mn?e[i]:-1,o=r!==-1?Hf(e,Ne+r):void 0,a=0;if(Xn(e,i,t)){let s=ce(null);try{if(o!==void 0&&V_(o,a),t!==-1){let l=Ne+t,c=Hf(e,l),u=Zv(e[ie],l),m=jx(c,u,e),v=Ts(e,u,n,{dehydratedView:m});As(c,v,a,Da(u,m))}}finally{ce(s)}}else if(o!==void 0){let s=kx(o,a);s!==void 0&&(s[wt]=n)}}var qv=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-pt}};function Ye(t,n){return n}var Yv=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Qe(t,n,e,i,r,o,a,s,l,c,u,m,v){fi("NgControlFlow");let _=le(),y=nt(),I=l!==void 0,R=le(),V=s?a.bind(R[Ut][wt]):a,_e=new Yv(I,V);R[Ne+t]=_e,Es(_,y,t+1,n,e,i,r,Qn(y.consts,o),256),I&&Es(_,y,t+2,l,c,u,m,Qn(y.consts,v),512)}var Qv=class extends Gv{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-pt}at(n){return this.getLView(n)[wt].$implicit}attach(n,e){let i=e[un];this.needsIndexUpdate||=n!==this.length,As(this.lContainer,e,n,Da(this.templateTNode,i)),GF(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,WF(this.lContainer,n),qF(this.lContainer,n)}create(n,e){let i=hc(this.lContainer,this.templateTNode.tView.ssrId);return Ts(this.hostLView,this.templateTNode,new qv(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Jf(n[ie],n)}updateValue(n,e){this.getLView(n)[wt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[wt].$index=n}getLView(n){return YF(this.lContainer,n)}};function Ze(t){let n=ce(null),e=pr();try{let i=le(),r=i[ie],o=i[e],a=e+1,s=Hf(i,a);if(o.liveCollection===void 0){let c=Zv(r,a);o.liveCollection=new Qv(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if($F(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Do(),u=l.length===0;if(Xn(i,c,u)){let m=e+2,v=Hf(i,m);if(u){let _=Zv(r,m),y=jx(v,_,i),I=Ts(i,_,void 0,{dehydratedView:y});As(v,I,0,Da(_,y))}else r.firstUpdatePass&&am(v),V_(v,0)}}}finally{ce(n)}}function Hf(t,n){return t[n]}function GF(t,n){if(t.length<=pt)return;let e=pt+n,i=t[e],r=i?i[bo]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Gn];vN(o,r),Ca.delete(i[Fr]),r.detachedLeaveAnimationFns=void 0}}function WF(t,n){if(t.length<=pt)return;let e=pt+n,i=t[e],r=i?i[bo]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function qF(t,n){return pc(t,n)}function YF(t,n){return kx(t,n)}function Zv(t,n){return ms(t,n)}function E(t,n,e){let i=le(),r=Do();if(Xn(i,r,n)){let o=nt(),a=rc();bx(a,i,t,n,i[Le],e)}return E}function Kv(t,n,e,i,r){F_(n,t,e,r?"class":"style",i)}function f(t,n,e,i){let r=le(),o=r[ie],a=t+Ne,s=o.firstCreatePass?B_(a,r,2,n,k_,nf(),e,i):o.data[a];if(mr(s)){let l=r[Ni].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(Qx(c),()=>(Nw(t,n,r,s,i),f))}}return Nw(t,n,r,s,i),f}function Nw(t,n,e,i,r){if(O_(i,e,t,n,iy),fs(i)){let o=e[ie];tm(o,e,i),f_(o,i,e)}r!=null&&xc(e,i)}function p(){let t=nt(),n=Pt(),e=N_(n);return t.firstCreatePass&&H_(t,e),Hg(e)&&Ug(),Bg(),e.classesWithoutHost!=null&&G1(e)&&Kv(t,e,le(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&W1(e)&&Kv(t,e,le(),e.stylesWithoutHost,!1),p}function z(t,n,e,i){return f(t,n,e,i),p(),z}function Et(t,n,e,i){let r=le(),o=r[ie],a=t+Ne,s=o.firstCreatePass?OP(a,o,2,n,e,i):o.data[a];return O_(s,r,t,n,iy),i!=null&&xc(r,s),Et}function Lt(){let t=Pt(),n=N_(t);return Hg(n)&&Ug(),Bg(),Lt}function Jt(t,n,e,i){return Et(t,n,e,i),Lt(),Jt}var iy=(t,n,e,i,r)=>(Lr(!0),y_(n[Le],i,df()));function QF(t,n,e,i,r){let o=!Yf(n,e);if(Lr(o),o)return y_(n[Le],i,df());let a=n[un],s=rm(a,t,n,e);return jE(a,r)&&qf(a,r,s.nextSibling),a&&(rE(e)||oE(s))&&mr(e)&&(ID(e),XE(s)),s}function wS(){iy=QF}function te(t,n,e){let i=le(),r=i[ie],o=t+Ne,a=r.firstCreatePass?B_(o,i,8,"ng-container",k_,nf(),n,e):r.data[o];if(O_(a,i,t,"ng-container",ES),fs(a)){let s=i[ie];tm(s,i,a),f_(s,a,i)}return e!=null&&xc(i,a),te}function ne(){let t=nt(),n=Pt(),e=N_(n);return t.firstCreatePass&&H_(t,e),ne}function it(t,n,e){return te(t,n,e),ne(),it}var ES=(t,n,e,i,r)=>(Lr(!0),ZE(n[Le],""));function ZF(t,n,e,i,r){let o,a=!Yf(n,e);if(Lr(a),a)return ZE(n[Le],"");let s=n[un],l=rm(s,t,n,e),c=CO(s,r);return qf(s,r,l),o=om(c,l),o}function xS(){ES=ZF}function Ke(){return le()}function xt(t,n,e){let i=le(),r=Do();if(Xn(i,r,n)){let o=nt(),a=rc();Cx(a,i,t,n,i[Le],e)}return xt}var ac=void 0;function KF(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var XF=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],ac,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],ac,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",ac,ac,ac],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",KF],dv={};function pn(t){let n=JF(t),e=Pw(n);if(e)return e;let i=n.split("-")[0];if(e=Pw(i),e)return e;if(i==="en")return XF;throw new P(701,!1)}function Pw(t){return t in dv||(dv[t]=ai.ng&&ai.ng.common&&ai.ng.common.locales&&ai.ng.common.locales[t]),dv[t]}var gt=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(gt||{});function JF(t){return t.toLowerCase().replace(/_/g,"-")}var Tc="en-US",eL="USD";var tL=Tc;function SS(t){typeof t=="string"&&(tL=t.toLowerCase().replace(/_/g,"-"))}function F(t,n,e){let i=le(),r=nt(),o=Pt();return MS(r,i,i[Le],o,t,n,e),F}function pm(t,n,e){let i=le(),r=nt(),o=Pt();return(o.type&3||e)&&qx(o,r,i,e,i[Le],t,n,Ef(o,i,n)),pm}function MS(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=Ef(i,n,o),qx(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let m=0;m<u.length;m+=2){let v=u[m],_=u[m+1];l??=Ef(i,n,o),yw(i,n,v,_,r,l)}if(c&&c.length)for(let m of c)l??=Ef(i,n,o),yw(i,n,m,r,r,l)}}function b(t=1){return BD(t)}function nL(t,n){let e=null,i=nN(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?ax(t,o,!0):oN(i,o))return r}return e}function Me(t){let n=le()[Ut][dn];if(!n.projection){let e=t?t.length:1,i=n.projection=uD(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?nL(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function K(t,n=0,e,i,r,o){let a=le(),s=nt(),l=i?t+1:null;l!==null&&Es(a,s,l,i,r,o,null,e);let c=Rs(s,Ne+t,16,null,e||null);c.projection===null&&(c.projection=n),Gg();let m=!a[un]||rf();a[Ut][dn].projection[c.projection]===null&&l!==null?iL(a,s,l):m&&!Dc(c)&&IN(s,a,c)}function iL(t,n,e){let i=Ne+e,r=n.data[i],o=t[i],a=hc(o,r.tView.ssrId),s=Ts(t,r,void 0,{dehydratedView:a});As(o,s,0,Da(r,a))}function rt(t,n,e,i){return rS(t,n,e,i),rt}function dt(t,n,e){return iS(t,n,e),dt}function Y(t){let n=le(),e=nt(),i=sf();ic(i+1);let r=W_(e,i);if(t.dirty&&CD(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=aS(n,i);t.reset(o,nE),t.notifyOnChanges()}return!0}return!1}function Q(){return G_(le(),sf())}function hm(t,n,e,i,r){return lS(n,rS(t,e,i,r)),hm}function gm(t,n,e,i){return lS(t,iS(n,e,i)),gm}function vm(t=1){ic(sf()+t)}function zt(t){let n=AD();return Ju(n,Ne+t)}function _f(t,n){return t<<17|n<<2}function wa(t){return t>>17&32767}function rL(t){return(t&2)==2}function oL(t,n){return t&131071|n<<17}function Xv(t){return t|2}function xs(t){return(t&131068)>>2}function uv(t,n){return t&-131069|n<<2}function aL(t){return(t&1)===1}function Jv(t){return t|1}function sL(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=wa(a),l=xs(a);t[i]=e;let c=!1,u;if(Array.isArray(e)){let m=e;u=m[1],(u===null||ls(m,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let v=wa(t[s+1]);t[i+1]=_f(v,s),v!==0&&(t[v+1]=uv(t[v+1],i)),t[s+1]=oL(t[s+1],i)}else t[i+1]=_f(s,0),s!==0&&(t[s+1]=uv(t[s+1],i)),s=i;else t[i+1]=_f(l,0),s===0?s=i:t[l+1]=uv(t[l+1],i),l=i;c&&(t[i+1]=Xv(t[i+1])),Fw(t,u,i,!0),Fw(t,u,i,!1),lL(n,u,t,i,o),a=_f(s,l),o?n.classBindings=a:n.styleBindings=a}function lL(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&ls(o,n)>=0&&(e[i+1]=Jv(e[i+1]))}function Fw(t,n,e,i){let r=t[e+1],o=n===null,a=i?wa(r):xs(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];cL(l,n)&&(s=!0,t[a+1]=i?Jv(c):Xv(c)),a=i?wa(c):xs(c)}s&&(t[e+1]=i?Xv(r):Jv(r))}function cL(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?ls(t,n)>=0:!1}var ji={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function dL(t){return t.substring(ji.key,ji.keyEnd)}function uL(t){return fL(t),IS(t,TS(t,0,ji.textEnd))}function IS(t,n){let e=ji.textEnd;return e===n?-1:(n=ji.keyEnd=mL(t,ji.key=n,e),TS(t,n,e))}function fL(t){ji.key=0,ji.keyEnd=0,ji.value=0,ji.valueEnd=0,ji.textEnd=t.length}function TS(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function mL(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function x(t,n,e){return AS(t,n,e,!1),x}function X(t,n){return AS(t,n,null,!0),X}function $t(t){hL(CL,pL,t,!0)}function pL(t,n){for(let e=uL(n);e>=0;e=IS(n,e))Qu(t,dL(n),!0)}function AS(t,n,e,i){let r=le(),o=nt(),a=of(2);if(o.firstUpdatePass&&kS(o,t,a,i),n!==mn&&Xn(r,a,n)){let s=o.data[pr()];OS(o,s,r,r[Le],t,r[a+1]=wL(n,e),i,a)}}function hL(t,n,e,i){let r=nt(),o=of(2);r.firstUpdatePass&&kS(r,null,o,i);let a=le();if(e!==mn&&Xn(a,o,e)){let s=r.data[pr()];if(NS(s,i)&&!RS(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=$u(l,e||"")),Kv(r,s,a,e,i)}else DL(r,s,a,a[Le],a[o+1],a[o+1]=bL(t,n,e),i,o)}}function RS(t,n){return n>=t.expandoStartIndex}function kS(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[pr()],a=RS(t,e);NS(o,i)&&n===null&&!a&&(n=!1),n=gL(r,o,n,i),sL(r,o,n,e,a,i)}}function gL(t,n,e,i){let r=FD(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=fv(null,t,n,e,i),e=vc(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=fv(r,t,n,e,i),o===null){let l=vL(t,n,i);l!==void 0&&Array.isArray(l)&&(l=fv(null,t,n,l[1],i),l=vc(l,n.attrs,i),_L(t,n,i,l))}else o=yL(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function vL(t,n,e){let i=e?n.classBindings:n.styleBindings;if(xs(i)!==0)return t[wa(i)]}function _L(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[wa(r)]=i}function yL(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=vc(i,a,e)}return vc(i,n.attrs,e)}function fv(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=vc(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function vc(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Qu(t,a,e?!0:n[++o]))}return t===void 0?null:t}function bL(t,n,e){if(e==null||e==="")return Kt;let i=[],r=Hi(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function CL(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Qu(t,i,e)}function DL(t,n,e,i,r,o,a,s){r===mn&&(r=Kt);let l=0,c=0,u=0<r.length?r[0]:null,m=0<o.length?o[0]:null;for(;u!==null||m!==null;){let v=l<r.length?r[l+1]:void 0,_=c<o.length?o[c+1]:void 0,y=null,I;u===m?(l+=2,c+=2,v!==_&&(y=m,I=_)):m===null||u!==null&&u<m?(l+=2,y=u):(c+=2,y=m,I=_),y!==null&&OS(t,n,e,i,y,I,a,s),u=l<r.length?r[l]:null,m=c<o.length?o[c]:null}}function OS(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],u=aL(c)?Lw(l,n,e,r,xs(c),a):void 0;if(!Uf(u)){Uf(o)||rL(c)&&(o=Lw(l,null,e,r,s,a));let m=Og(pr(),e);AN(i,a,m,r,o)}}function Lw(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,m=u===null,v=e[r+1];v===mn&&(v=m?Kt:void 0);let _=m?Zu(v,i):u===i?v:void 0;if(c&&!Uf(_)&&(_=Zu(l,i)),Uf(_)&&(s=_,a))return s;let y=t[r+1];r=a?wa(y):xs(y)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=Zu(l,i))}return s}function Uf(t){return t!==void 0}function wL(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Zl(Hi(t)))),t}function NS(t,n){return(t.flags&(n?8:16))!==0}function g(t,n=""){let e=le(),i=nt(),r=t+Ne,o=i.firstCreatePass?Rs(i,r,1,n,null):i.data[r],a=PS(i,e,o,n);e[r]=a,uf()&&A_(i,e,a,o),ps(o,!1)}var PS=(t,n,e,i)=>(Lr(!0),QE(n[Le],i));function EL(t,n,e,i){let r=!Yf(n,e);if(Lr(r),r)return QE(n[Le],i);let o=n[un];return rm(o,t,n,e)}function FS(){PS=EL}function LS(t,n,e,i=""){return Xn(t,Do(),e)?n+ss(e)+i:mn}function xL(t,n,e,i,r,o=""){let a=RD(),s=z_(t,a,e,r);return of(2),s?n+ss(e)+i+ss(r)+o:mn}function me(t){return $("",t),me}function $(t,n,e){let i=le(),r=LS(i,t,n,e);return r!==mn&&VS(i,pr(),r),$}function Cr(t,n,e,i,r){let o=le(),a=xL(o,t,n,e,i,r);return a!==mn&&VS(o,pr(),a),Cr}function VS(t,n,e){let i=Og(n,t);WO(t[Le],i,e)}function _m(t,n,e){K_(n)&&(n=n());let i=le(),r=Do();if(Xn(i,r,n)){let o=nt(),a=rc();bx(a,i,t,n,i[Le],e)}return _m}function ry(t,n){let e=K_(t);return e&&t.set(n),e}function ym(t,n){let e=le(),i=nt(),r=Pt();return MS(i,e,e[Le],r,t,n),ym}function oy(t,n,e=""){return LS(le(),t,n,e)}function Vw(t,n,e){let i=nt();i.firstCreatePass&&jS(n,i.data,i.blueprint,Fi(t),e)}function jS(t,n,e,i,r){if(t=Nt(t),Array.isArray(t))for(let o=0;o<t.length;o++)jS(t[o],n,e,i,r);else{let o=nt(),a=le(),s=Pt(),l=ca(t)?t:Nt(t.provide),c=Ig(t),u=s.providerIndexes&1048575,m=s.directiveStart,v=s.providerIndexes>>20;if(ca(t)||!t.multi){let _=new ba(c,r,ee,null),y=pv(l,n,r?u:u+v,m);y===-1?(gv(Af(s,a),o,l),mv(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(_),a.push(_)):(e[y]=_,a[y]=_)}else{let _=pv(l,n,u+v,m),y=pv(l,n,u,u+v),I=_>=0&&e[_],R=y>=0&&e[y];if(r&&!R||!r&&!I){gv(Af(s,a),o,l);let V=IL(r?ML:SL,e.length,r,i,c,t);!r&&R&&(e[y].providerFactory=V),mv(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(V),a.push(V)}else{let V=BS(e[r?y:_],c,!r&&i);mv(o,t,_>-1?_:y,V)}!r&&i&&R&&e[y].componentProviders++}}}function mv(t,n,e,i){let r=ca(n),o=vD(n);if(r||o){let l=(o?Nt(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function BS(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function pv(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function SL(t,n,e,i,r){return e_(this.multi,[])}function ML(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=dc(i,i[ie],this.providerFactory.index,r);a=l.slice(0,s),e_(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],e_(o,a);return a}function e_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function IL(t,n,e,i,r,o){let a=new ba(t,e,ee,null);return a.multi=[],a.index=n,a.componentProviders=0,BS(a,r,i&&!e),a}function we(t,n){return e=>{e.providersResolver=(i,r)=>Vw(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Vw(i,r?r(n):n,!0))}}function ot(t,n){let e=hs()+t,i=le();return i[e]===mn?dm(i,e,n()):NP(i,e)}function Ps(t,n,e){return HS(le(),hs(),t,n,e)}function ay(t,n,e,i,r){return AL(le(),hs(),t,n,e,i,r)}function sy(t,n){let e=t[n];return e===mn?void 0:e}function HS(t,n,e,i,r,o){let a=n+e;return Xn(t,a,r)?dm(t,a+1,o?i.call(o,r):i(r)):sy(t,a+1)}function TL(t,n,e,i,r,o,a){let s=n+e;return z_(t,s,r,o)?dm(t,s+2,a?i.call(a,r,o):i(r,o)):sy(t,s+2)}function AL(t,n,e,i,r,o,a,s){let l=n+e;return PP(t,l,r,o,a)?dm(t,l+3,s?i.call(s,r,o,a):i(r,o,a)):sy(t,l+3)}function Gt(t,n){let e=nt(),i,r=t+Ne;e.firstCreatePass?(i=RL(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=go(i.type,!0)),a,s=sn(ee);try{let l=Tf(!1),c=o();return Tf(l),Ng(e,le(),r,c),c}finally{sn(s)}}function RL(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function mi(t,n,e){let i=t+Ne,r=le(),o=Ju(r,i);return US(r,i)?HS(r,hs(),n,o.transform,e,o):o.transform(e)}function Fs(t,n,e,i){let r=t+Ne,o=le(),a=Ju(o,r);return US(o,r)?TL(o,hs(),n,a.transform,e,i,a):a.transform(e,i)}function US(t,n){return t[ie].data[n].pure}function Ls(t,n){return im(t,n)}var zf=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},ly=(()=>{class t{compileModuleSync(e){return new jf(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=bg(e),o=rx(r.declarations).reduce((a,s)=>{let l=ki(s);return l&&a.push(new xo(l)),a},[]);return new zf(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var zS=(()=>{class t{applicationErrorHandler=d(Zn);appRef=d(At);taskService=d(gr);ngZone=d(re);zonelessEnabled=d(oc);tracing=d(Ui,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Yl):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(ev,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?$D:Zg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Yl+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function $S(){return[{provide:Ri,useExisting:zS},{provide:re,useClass:Ql},{provide:oc,useValue:!0}]}function kL(){return typeof $localize<"u"&&$localize.locale||Tc}var Vs=new C("",{factory:()=>d(Vs,{optional:!0,skipSelf:!0})||kL()}),cy=new C("",{factory:()=>eL});function He(t){return eD(t)}function Rt(t,n){return Rl(t,n?.equal)}var OL=t=>t;function bm(t,n){if(typeof t=="function"){let e=eg(t,OL,n?.equal);return GS(e,n?.debugName)}else{let e=eg(t.source,t.computation,t.equal);return GS(e,t.debugName)}}function GS(t,n){let e=t[bt],i=t;return i.set=r=>XC(e,r),i.update=r=>JC(e,r),i.asReadonly=ff.bind(t),i}var py={JSACTION:"__jsaction",OWNER:"__owner"},QS={};function NL(t){return t[py.JSACTION]}function WS(t,n){t[py.JSACTION]=n}function PL(t){return QS[t]}function FL(t,n){QS[t]=n}var ue={CLICK:"click",CLICKMOD:"clickmod",DBLCLICK:"dblclick",FOCUS:"focus",FOCUSIN:"focusin",BLUR:"blur",FOCUSOUT:"focusout",SUBMIT:"submit",KEYDOWN:"keydown",KEYPRESS:"keypress",KEYUP:"keyup",MOUSEOVER:"mouseover",MOUSEOUT:"mouseout",MOUSEENTER:"mouseenter",MOUSELEAVE:"mouseleave",POINTEROVER:"pointerover",POINTEROUT:"pointerout",POINTERENTER:"pointerenter",POINTERLEAVE:"pointerleave",ERROR:"error",LOAD:"load",TOUCHSTART:"touchstart",TOUCHEND:"touchend",TOUCHMOVE:"touchmove",TOGGLE:"toggle"},LL=[ue.MOUSEENTER,ue.MOUSELEAVE,"pointerenter","pointerleave"],wK=[ue.CLICK,ue.DBLCLICK,ue.FOCUSIN,ue.FOCUSOUT,ue.KEYDOWN,ue.KEYUP,ue.KEYPRESS,ue.MOUSEOVER,ue.MOUSEOUT,ue.SUBMIT,ue.TOUCHSTART,ue.TOUCHEND,ue.TOUCHMOVE,"touchcancel","auxclick","change","compositionstart","compositionupdate","compositionend","beforeinput","input","select","copy","cut","paste","mousedown","mouseup","wheel","contextmenu","dragover","dragenter","dragleave","drop","dragstart","dragend","pointerdown","pointermove","pointerup","pointercancel","pointerover","pointerout","gotpointercapture","lostpointercapture","ended","loadedmetadata","pagehide","pageshow","visibilitychange","beforematch"],VL=[ue.FOCUS,ue.BLUR,ue.ERROR,ue.LOAD,ue.TOGGLE],hy=t=>VL.indexOf(t)>=0;function jL(t){return t===ue.MOUSEENTER?ue.MOUSEOVER:t===ue.MOUSELEAVE?ue.MOUSEOUT:t===ue.POINTERENTER?ue.POINTEROVER:t===ue.POINTERLEAVE?ue.POINTEROUT:t}function BL(t,n,e,i){let r=!1;hy(n)&&(r=!0);let o=typeof i=="boolean"?{capture:r,passive:i}:r;return t.addEventListener(n,e,o),{eventType:n,handler:e,capture:r,passive:i}}function HL(t,n){if(t.removeEventListener){let e=typeof n.passive=="boolean"?{capture:n.capture}:n.capture;t.removeEventListener(n.eventType,n.handler,e)}else t.detachEvent&&t.detachEvent(`on${n.eventType}`,n.handler)}function UL(t){t.preventDefault?t.preventDefault():t.returnValue=!1}var qS=typeof navigator<"u"&&/Macintosh/.test(navigator.userAgent);function zL(t){return t.which===2||t.which==null&&t.button===4}function $L(t){return qS&&t.metaKey||!qS&&t.ctrlKey||zL(t)||t.shiftKey}function GL(t,n,e){let i=t.relatedTarget;return(t.type===ue.MOUSEOVER&&n===ue.MOUSEENTER||t.type===ue.MOUSEOUT&&n===ue.MOUSELEAVE||t.type===ue.POINTEROVER&&n===ue.POINTERENTER||t.type===ue.POINTEROUT&&n===ue.POINTERLEAVE)&&(!i||i!==e&&!e.contains(i))}function WL(t,n){let e={};for(let i in t){if(i==="srcElement"||i==="target")continue;let r=i,o=t[r];typeof o!="function"&&(e[r]=o)}return t.type===ue.MOUSEOVER?e.type=ue.MOUSEENTER:t.type===ue.MOUSEOUT?e.type=ue.MOUSELEAVE:t.type===ue.POINTEROVER?e.type=ue.POINTERENTER:e.type=ue.POINTERLEAVE,e.target=e.srcElement=n,e.bubbles=!1,e._originalEvent=t,e}var Em=class{element;handlerInfos=[];constructor(n){this.element=n}addEventListener(n,e,i){this.handlerInfos.push(BL(this.element,n,e(this.element),i))}cleanUp(){for(let n=0;n<this.handlerInfos.length;n++)HL(this.element,this.handlerInfos[n]);this.handlerInfos=[]}},qL={EVENT_ACTION_SEPARATOR:":"};function Io(t){return t.eventType}function gy(t,n){t.eventType=n}function Dm(t){return t.event}function ZS(t,n){t.event=n}function KS(t){return t.targetElement}function XS(t,n){t.targetElement=n}function JS(t){return t.eic}function YL(t,n){t.eic=n}function QL(t){return t.timeStamp}function ZL(t,n){t.timeStamp=n}function wm(t){return t.eia}function eM(t,n,e){t.eia=[n,e]}function dy(t){t.eia=void 0}function Cm(t){return t[1]}function KL(t){return t.eirp}function tM(t,n){t.eirp=n}function nM(t){return t.eir}function iM(t,n){t.eir=n}function rM(t){return{eventType:t.eventType,event:t.event,targetElement:t.targetElement,eic:t.eic,eia:t.eia,timeStamp:t.timeStamp,eirp:t.eirp,eiack:t.eiack,eir:t.eir}}function XL(t,n,e,i,r,o,a,s){return{eventType:t,event:n,targetElement:e,eic:i,timeStamp:r,eia:o,eirp:a,eiack:s}}var uy=class t{eventInfo;constructor(n){this.eventInfo=n}getEventType(){return Io(this.eventInfo)}setEventType(n){gy(this.eventInfo,n)}getEvent(){return Dm(this.eventInfo)}setEvent(n){ZS(this.eventInfo,n)}getTargetElement(){return KS(this.eventInfo)}setTargetElement(n){XS(this.eventInfo,n)}getContainer(){return JS(this.eventInfo)}setContainer(n){YL(this.eventInfo,n)}getTimestamp(){return QL(this.eventInfo)}setTimestamp(n){ZL(this.eventInfo,n)}getAction(){let n=wm(this.eventInfo);if(n)return{name:n[0],element:n[1]}}setAction(n){if(!n){dy(this.eventInfo);return}eM(this.eventInfo,n.name,n.element)}getIsReplay(){return KL(this.eventInfo)}setIsReplay(n){tM(this.eventInfo,n)}getResolved(){return nM(this.eventInfo)}setResolved(n){iM(this.eventInfo,n)}clone(){return new t(rM(this.eventInfo))}},JL={},e2=/\s*;\s*/,t2=ue.CLICK,fy=class{a11yClickSupport=!1;clickModSupport=!0;syntheticMouseEventSupport;updateEventInfoForA11yClick=void 0;preventDefaultForA11yClick=void 0;populateClickOnlyAction=void 0;constructor({syntheticMouseEventSupport:n=!1,clickModSupport:e=!0}={}){this.syntheticMouseEventSupport=n,this.clickModSupport=e}resolveEventType(n){this.clickModSupport&&Io(n)===ue.CLICK&&$L(Dm(n))?gy(n,ue.CLICKMOD):this.a11yClickSupport&&this.updateEventInfoForA11yClick(n)}resolveAction(n){nM(n)||(this.populateAction(n,KS(n)),iM(n,!0))}resolveParentAction(n){let e=wm(n),i=e&&Cm(e);dy(n);let r=i&&this.getParentNode(i);r&&this.populateAction(n,r)}populateAction(n,e){let i=e;for(;i&&i!==JS(n)&&(i.nodeType===Node.ELEMENT_NODE&&this.populateActionOnElement(i,n),!wm(n));)i=this.getParentNode(i);let r=wm(n);if(r&&(this.a11yClickSupport&&this.preventDefaultForA11yClick(n),this.syntheticMouseEventSupport&&(Io(n)===ue.MOUSEENTER||Io(n)===ue.MOUSELEAVE||Io(n)===ue.POINTERENTER||Io(n)===ue.POINTERLEAVE)))if(GL(Dm(n),Io(n),Cm(r))){let o=WL(Dm(n),Cm(r));ZS(n,o),XS(n,Cm(r))}else dy(n)}getParentNode(n){let e=n[py.OWNER];if(e)return e;let i=n.parentNode;return i?.nodeName==="#document-fragment"?i?.host??null:i}populateActionOnElement(n,e){let i=this.parseActions(n),r=i[Io(e)];r!==void 0&&eM(e,r,n),this.a11yClickSupport&&this.populateClickOnlyAction(n,e,i)}parseActions(n){let e=NL(n);if(!e){let i=n.getAttribute(pf.JSACTION);if(!i)e=JL,WS(n,e);else{if(e=PL(i),!e){e={};let r=i.split(e2);for(let o=0;o<r.length;o++){let a=r[o];if(!a)continue;let s=a.indexOf(qL.EVENT_ACTION_SEPARATOR),l=s!==-1,c=l?a.substr(0,s).trim():t2,u=l?a.substr(s+1).trim():a;e[c]=u}FL(i,e)}WS(n,e)}}return e}addA11yClickSupport(n,e,i){this.a11yClickSupport=!0,this.updateEventInfoForA11yClick=n,this.preventDefaultForA11yClick=e,this.populateClickOnlyAction=i}},oM=(function(t){return t[t.I_AM_THE_JSACTION_FRAMEWORK=0]="I_AM_THE_JSACTION_FRAMEWORK",t})(oM||{}),my=class{dispatchDelegate;actionResolver;eventReplayer;eventReplayScheduled=!1;replayEventInfoWrappers=[];constructor(n,{actionResolver:e,eventReplayer:i}={}){this.dispatchDelegate=n,this.actionResolver=e,this.eventReplayer=i}dispatch(n){let e=new uy(n);this.actionResolver?.resolveEventType(n),this.actionResolver?.resolveAction(n);let i=e.getAction();if(i&&n2(i.element,e)&&UL(e.getEvent()),this.eventReplayer&&e.getIsReplay()){this.scheduleEventInfoWrapperReplay(e);return}this.dispatchDelegate(e)}scheduleEventInfoWrapperReplay(n){this.replayEventInfoWrappers.push(n),!this.eventReplayScheduled&&(this.eventReplayScheduled=!0,Promise.resolve().then(()=>{this.eventReplayScheduled=!1,this.eventReplayer(this.replayEventInfoWrappers)}))}};function n2(t,n){return t.tagName==="A"&&(n.getEventType()===ue.CLICK||n.getEventType()===ue.CLICKMOD)}var aM=Symbol.for("propagationStopped"),vy={REPLAY:101};var i2="`preventDefault` called during event replay.";var r2="`composedPath` called during event replay.",xm=class{dispatchDelegate;clickModSupport;actionResolver;dispatcher;constructor(n,e=!0){this.dispatchDelegate=n,this.clickModSupport=e,this.actionResolver=new fy({clickModSupport:e}),this.dispatcher=new my(i=>{this.dispatchToDelegate(i)},{actionResolver:this.actionResolver})}dispatch(n){this.dispatcher.dispatch(n)}dispatchToDelegate(n){for(n.getIsReplay()&&s2(n),o2(n);n.getAction();){if(l2(n),hy(n.getEventType())&&n.getAction().element!==n.getTargetElement()||(this.dispatchDelegate(n.getEvent(),n.getAction().name),a2(n)))return;this.actionResolver.resolveParentAction(n.eventInfo)}}};function o2(t){let n=t.getEvent(),e=t.getEvent().stopPropagation.bind(n),i=()=>{n[aM]=!0,e()};xa(n,"stopPropagation",i),xa(n,"stopImmediatePropagation",i)}function a2(t){return!!t.getEvent()[aM]}function s2(t){let n=t.getEvent(),e=t.getTargetElement(),i=n.preventDefault.bind(n);xa(n,"target",e),xa(n,"eventPhase",vy.REPLAY),xa(n,"preventDefault",()=>{throw i(),new Error(i2+"")}),xa(n,"composedPath",()=>{throw new Error(r2+"")})}function l2(t){let n=t.getEvent(),e=t.getAction()?.element;e&&xa(n,"currentTarget",e,{configurable:!0})}function xa(t,n,e,{configurable:i=!1}={}){Object.defineProperty(t,n,{value:e,configurable:i})}function sM(t,n){t.ecrd(e=>{n.dispatch(e)},oM.I_AM_THE_JSACTION_FRAMEWORK)}function c2(t){return t?.q??[]}function d2(t){t&&(YS(t.c,t.et,t.h),YS(t.c,t.etc,t.h,!0))}function YS(t,n,e,i){for(let r=0;r<n.length;r++)t.removeEventListener(n[r],e,i)}var u2=!1,lM=(()=>{class t{static MOUSE_SPECIAL_SUPPORT=u2;containerManager;eventHandlers={};browserEventTypeToExtraEventTypes={};dispatcher=null;queuedEventInfos=[];constructor(e){this.containerManager=e}handleEvent(e,i,r){let o=XL(e,i,i.target,r,Date.now());this.handleEventInfo(o)}handleEventInfo(e){if(!this.dispatcher){tM(e,!0),this.queuedEventInfos?.push(e);return}this.dispatcher(e)}addEvent(e,i,r){if(e in this.eventHandlers||!this.containerManager||!t.MOUSE_SPECIAL_SUPPORT&&LL.indexOf(e)>=0)return;let o=(s,l,c)=>{this.handleEvent(s,l,c)};this.eventHandlers[e]=o;let a=jL(i||e);if(a!==e){let s=this.browserEventTypeToExtraEventTypes[a]||[];s.push(e),this.browserEventTypeToExtraEventTypes[a]=s}this.containerManager.addEventListener(a,s=>l=>{o(e,l,s)},r)}replayEarlyEvents(e=window._ejsa){e&&(this.replayEarlyEventInfos(e.q),d2(e),delete window._ejsa)}replayEarlyEventInfos(e){for(let i=0;i<e.length;i++){let r=e[i],o=this.getEventTypesForBrowserEventType(r.eventType);for(let a=0;a<o.length;a++){let s=rM(r);gy(s,o[a]),this.handleEventInfo(s)}}}getEventTypesForBrowserEventType(e){let i=[];return this.eventHandlers[e]&&i.push(e),this.browserEventTypeToExtraEventTypes[e]&&i.push(...this.browserEventTypeToExtraEventTypes[e]),i}handler(e){return this.eventHandlers[e]}cleanUp(){this.containerManager?.cleanUp(),this.containerManager=null,this.eventHandlers={},this.browserEventTypeToExtraEventTypes={},this.dispatcher=null,this.queuedEventInfos=[]}registerDispatcher(e,i){this.ecrd(e,i)}ecrd(e,i){if(this.dispatcher=e,this.queuedEventInfos?.length){for(let r=0;r<this.queuedEventInfos.length;r++)this.handleEventInfo(this.queuedEventInfos[r]);this.queuedEventInfos=null}}}return t})();function cM(t,n=window){return c2(n._ejsas?.[t])}function _y(t,n=window){n._ejsas&&(n._ejsas[t]=void 0)}var bM=Symbol("InputSignalNode#UNSET"),S2=fe(D({},kl),{transformFn:void 0,applyValueToInputSignal(t,n){Xo(t,n)}});function CM(t,n){let e=Object.create(S2);e.value=t,e.transformFn=n?.transform;function i(){if(ao(e),e.value===bM){let r=null;throw new P(-950,r)}return e.value}return i[bt]=e,i}var kn=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>yc(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function dM(t,n){return CM(t,n)}function M2(t){return CM(bM,t)}var Tm=(dM.required=M2,dM);function uM(t,n){return q_(n)}function I2(t,n){return Y_(n)}var Rc=(uM.required=I2,uM);function fM(t,n){return q_(n)}function T2(t,n){return Y_(n)}var DM=(fM.required=T2,fM);var by=new C(""),A2=new C("");function Ac(t){return!t.moduleRef}function R2(t){let n=Ac(t)?t.r3Injector:t.moduleRef.injector,e=n.get(re);return e.run(()=>{Ac(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Zn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ac(t)){let o=()=>n.destroy(),a=t.platformInjector.get(by);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(by);a.add(o),t.moduleRef.onDestroy(()=>{cc(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return O2(i,e,()=>{let o=n.get(gr),a=o.add(),s=n.get(ey);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(Vs,Tc);if(SS(l||Tc),!n.get(A2,!0))return Ac(t)?n.get(At):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ac(t)){let u=n.get(At);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return k2?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var k2;function O2(t,n,e){try{let i=e();return So(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Mm=null;function N2(t=[],n){return oe.create({name:n,providers:[{provide:ec,useValue:"platform"},{provide:by,useValue:new Set([()=>Mm=null])},...t]})}function P2(t=[]){if(Mm)return Mm;let n=N2(t);return Mm=n,yS(),F2(n),n}function F2(t){let n=t.get($f,null);Bt(t,()=>{n?.forEach(e=>e())})}var Sm=new WeakSet,mM="";function pM(t){return t.get(s_,DE)}function wM(){let t=[{provide:s_,useFactory:()=>{let n=!0;{let e=d(ui);n=!!window._ejsas?.[e]}return n&&fi("NgEventReplay"),n}}];return t.push({provide:cr,useValue:()=>{let n=d(At),{injector:e}=n;if(!Sm.has(n)){let i=d(l_);if(pM(e)){RE();let r=e.get(ui),o=TE(r,(a,s,l)=>{a.nodeType===Node.ELEMENT_NODE&&(xE(a,s,l),SE(a,i))});n.onDestroy(o)}}},multi:!0},{provide:Mo,useFactory:()=>{let n=d(At),{injector:e}=n;return()=>{if(!pM(e)||Sm.has(n))return;Sm.add(n);let i=e.get(ui);n.onDestroy(()=>{Sm.delete(n),_y(i)}),n.whenStable().then(()=>{if(n.destroyed)return;let r=e.get(IE);L2(r,e);let o=e.get(l_);o.get(mM)?.forEach(ME),o.delete(mM);let a=r.instance;VE(e)?n.onDestroy(()=>a.cleanUp()):a.cleanUp()})}},multi:!0}),t}var L2=(t,n)=>{let e=n.get(ui),i=window._ejsas[e],r=t.instance=new lM(new Em(i.c));for(let s of i.et)r.addEvent(s);for(let s of i.etc)r.addEvent(s);let o=cM(e);r.replayEarlyEventInfos(o),_y(e);let a=new xm(s=>{V2(n,s,s.currentTarget)});sM(r,a)};function V2(t,n,e){let i=(e&&e.getAttribute(Cc))??"";/d\d+/.test(i)?j2(i,t,n,e):n.eventPhase===vy.REPLAY&&c_(n,e)}function j2(t,n,e,i){let r=n.get(wE);r.push({event:e,currentTarget:i}),DS(n,t,B2(r))}function B2(t){return n=>{let e=new Set(n),i=[];for(let{event:r,currentTarget:o}of t){let a=o.getAttribute(Cc);e.has(a)?c_(r,o):i.push({event:r,currentTarget:o})}t.length=0,t.push(...i)}}var hM=!1;var H2=1e4;function U2(){hM||(hM=!0,PE(),wS(),FS(),xS(),hS(),eS(),Vx(),yx())}function z2(t){return t.whenStable()}function EM(){let t=[{provide:Is,useFactory:()=>{let n=!0;return n=!!d(Ms,{optional:!0})?.get(d_,null),n&&fi("NgHydration"),n}},{provide:cr,useValue:()=>{Nx(!1);let n=d(j);d(Is)&&(HE(n),U2())},multi:!0}];return t.push({provide:a_,useFactory:()=>d(Is)},{provide:Mo,useFactory:()=>{let n=d(Ri);if(d(Is)){let e=d(At);return()=>{z2(e).then(()=>{e.destroyed||(j_(e),n.notify(7))})}}return()=>{}},multi:!0}),dr(t)}var Xte=H2-1e3;var Fe=(()=>{class t{static __NG_ELEMENT_ID__=$2}return t})();function $2(t){return G2(Pt(),le(),(t&16)===16)}function G2(t,n,e){if(mr(t)&&!e){let i=di(t.index,n);return new Eo(i,i)}else if(t.type&175){let i=n[Ut];return new Eo(i,n)}return null}var Cy=class{supports(n){return U_(n)}create(n){return new Dy(n)}},W2=(t,n)=>n,Dy=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||W2}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let a=!i||e&&e.currentIndex<gM(i,r,o)?e:i,s=gM(a,r,o),l=a.currentIndex;if(a===i)r--,i=i._nextRemoved;else if(e=e._next,a.previousIndex==null)r++;else{o||(o=[]);let c=s-r,u=l-r;if(c!=u){for(let v=0;v<c;v++){let _=v<o.length?o[v]:o[v]=0,y=_+v;u<=y&&y<c&&(o[v]=_+1)}let m=a.previousIndex;o[m]=u-c}}s!==l&&n(a,s,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!U_(n))throw new P(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,a;if(Array.isArray(n)){this.length=n.length;for(let s=0;s<this.length;s++)o=n[s],a=this._trackByFn(s,o),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,o,a,s),i=!0):(i&&(e=this._verifyReinsertion(e,o,a,s)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,Gx(n,s=>{a=this._trackByFn(r,s),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,s,a,r),i=!0):(i&&(e=this._verifyReinsertion(e,s,a,r)),Object.is(e.item,s)||this._addIdentityChange(e,s)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new wy(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Im),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Im),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},wy=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},Ey=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Im=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new Ey,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function gM(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function vM(){return new Ur([new Cy])}var Ur=(()=>{class t{factories;static \u0275prov=w({token:t,providedIn:"root",factory:vM});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||vM())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new P(901,!1)}}return t})();function xM(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Be(Oe.BootstrapApplicationStart);try{let o=r?.injector??P2(i),a=[$S(),WD,...e||[]],s=new gc({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return R2({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Be(Oe.BootstrapApplicationEnd)}}function q(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function zi(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var yy=Symbol("NOT_SET"),SM=new Set,q2=fe(D({},kl),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:yy,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==yy&&!Za(this))return this.signal;try{for(let r of this.cleanup??SM)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=kr(this),i;try{i=this.userFn.apply(null,n)}finally{so(this,e)}return(this.value===yy||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),xy=class extends fc{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(fn),a),this.scheduler=r;for(let s of M_){let l=e[s];if(l===void 0)continue;let c=Object.create(q2);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(ao(c),c.value),c.signal[bt]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[s]=c,this.hooks[s]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??SM)e()}finally{lo(n)}}};function MM(t,n){let e=n?.injector??d(oe),i=e.get(Ri),r=e.get(Xf),o=e.get(Ui,null,{optional:!0});r.impl??=e.get(I_);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(gs,null,{optional:!0}),l=new xy(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Am(t,n){let e=ki(t),i=n.elementInjector||cs();return new xo(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}function IM(t){let n=ki(t);if(!n)return null;let e=new xo(n);return{get selector(){return e.selector},get type(){return e.componentType},get inputs(){return e.inputs},get outputs(){return e.outputs},get ngContentSelectors(){return e.ngContentSelectors},get isStandalone(){return n.standalone},get isSignal(){return n.signals}}}var TM=null;function pi(){return TM}function Sy(t){TM??=t}var kc=class{},Rm=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(AM),providedIn:"platform"})}return t})();var AM=(()=>{class t extends Rm{_location;_history;_doc=d(j);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return pi().getBaseHref(this._doc)}onPopState(e){let i=pi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=pi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function OM(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function RM(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Ao(t){return t&&t[0]!=="?"?`?${t}`:t}var js=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(Q2),providedIn:"root"})}return t})(),Y2=new C(""),Q2=(()=>{class t extends js{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(j).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return OM(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Ao(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+Ao(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+Ao(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(se(Rm),se(Y2,8))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ro=(()=>{class t{_subject=new k;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=X2(RM(kM(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Ao(i))}normalize(e){return t.stripTrailingSlash(K2(this._basePath,kM(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Ao(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Ao(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Ao;static joinWithSlash=OM;static stripTrailingSlash=RM;static \u0275fac=function(i){return new(i||t)(se(js))};static \u0275prov=w({token:t,factory:()=>Z2(),providedIn:"root"})}return t})();function Z2(){return new Ro(se(js))}function K2(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function kM(t){return t.replace(/\/index.html$/,"")}function X2(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var LM={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},ky=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(ky||{});var en=(function(t){return t[t.Format=0]="Format",t[t.Standalone=1]="Standalone",t})(en||{}),Xe=(function(t){return t[t.Narrow=0]="Narrow",t[t.Abbreviated=1]="Abbreviated",t[t.Wide=2]="Wide",t[t.Short=3]="Short",t})(Xe||{}),On=(function(t){return t[t.Short=0]="Short",t[t.Medium=1]="Medium",t[t.Long=2]="Long",t[t.Full=3]="Full",t})(On||{}),Nn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function VM(t){return pn(t)[gt.LocaleId]}function jM(t,n,e){let i=pn(t),r=[i[gt.DayPeriodsFormat],i[gt.DayPeriodsStandalone]],o=hi(r,n);return hi(o,e)}function BM(t,n,e){let i=pn(t),r=[i[gt.DaysFormat],i[gt.DaysStandalone]],o=hi(r,n);return hi(o,e)}function HM(t,n,e){let i=pn(t),r=[i[gt.MonthsFormat],i[gt.MonthsStandalone]],o=hi(r,n);return hi(o,e)}function UM(t,n){let i=pn(t)[gt.Eras];return hi(i,n)}function Oc(t,n){let e=pn(t);return hi(e[gt.DateFormat],n)}function Nc(t,n){let e=pn(t);return hi(e[gt.TimeFormat],n)}function Pc(t,n){let i=pn(t)[gt.DateTimeFormat];return hi(i,n)}function Dr(t,n){let e=pn(t),i=e[gt.NumberSymbols][n];if(typeof i>"u"){if(n===Nn.CurrencyDecimal)return e[gt.NumberSymbols][Nn.Decimal];if(n===Nn.CurrencyGroup)return e[gt.NumberSymbols][Nn.Group]}return i}function zM(t,n){return pn(t)[gt.NumberFormats][n]}function J2(t){return pn(t)[gt.Currencies]}function $M(t){if(!t[gt.ExtraData])throw new P(2303,!1)}function GM(t){let n=pn(t);return $M(n),(n[gt.ExtraData][2]||[]).map(i=>typeof i=="string"?My(i):[My(i[0]),My(i[1])])}function WM(t,n,e){let i=pn(t);$M(i);let r=[i[gt.ExtraData][0],i[gt.ExtraData][1]],o=hi(r,n)||[];return hi(o,e)||[]}function hi(t,n){for(let e=n;e>-1;e--)if(typeof t[e]<"u")return t[e];throw new P(2304,!1)}function My(t){let[n,e]=t.split(":");return{hours:+n,minutes:+e}}function qM(t,n,e="en"){let i=J2(e)[t]||LM[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var eV=2;function YM(t){let n,e=LM[t];return e&&(n=e[2]),typeof n=="number"?n:eV}var tV=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,km={},nV=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/;function QM(t,n,e,i){let r=uV(t);n=zr(e,n)||n;let a=[],s;for(;n;)if(s=nV.exec(n),s){a=a.concat(s.slice(1));let u=a.pop();if(!u)break;n=u}else{a.push(n);break}let l=r.getTimezoneOffset();i&&(l=KM(i,l),r=dV(r,i));let c="";return a.forEach(u=>{let m=lV(u);c+=m?m(r,e,l):u==="''"?"'":u.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),c}function Lm(t,n,e){let i=new Date(0);return i.setFullYear(t,n,e),i.setHours(0,0,0),i}function zr(t,n){let e=VM(t);if(km[e]??={},km[e][n])return km[e][n];let i="";switch(n){case"shortDate":i=Oc(t,On.Short);break;case"mediumDate":i=Oc(t,On.Medium);break;case"longDate":i=Oc(t,On.Long);break;case"fullDate":i=Oc(t,On.Full);break;case"shortTime":i=Nc(t,On.Short);break;case"mediumTime":i=Nc(t,On.Medium);break;case"longTime":i=Nc(t,On.Long);break;case"fullTime":i=Nc(t,On.Full);break;case"short":let r=zr(t,"shortTime"),o=zr(t,"shortDate");i=Om(Pc(t,On.Short),[r,o]);break;case"medium":let a=zr(t,"mediumTime"),s=zr(t,"mediumDate");i=Om(Pc(t,On.Medium),[a,s]);break;case"long":let l=zr(t,"longTime"),c=zr(t,"longDate");i=Om(Pc(t,On.Long),[l,c]);break;case"full":let u=zr(t,"fullTime"),m=zr(t,"fullDate");i=Om(Pc(t,On.Full),[u,m]);break}return i&&(km[e][n]=i),i}function Om(t,n){return n&&(t=t.replace(/\{([^}]+)}/g,function(e,i){return n!=null&&i in n?n[i]:e})),t}function $i(t,n,e="-",i,r){let o="";(t<0||r&&t<=0)&&(r?t=-t+1:(t=-t,o=e));let a=String(t);for(;a.length<n;)a="0"+a;return i&&(a=a.slice(a.length-n)),o+a}function iV(t,n){return $i(t,3).substring(0,n)}function kt(t,n,e=0,i=!1,r=!1){return function(o,a){let s=rV(t,o);if((e>0||s>-e)&&(s+=e),t===3)s===0&&e===-12&&(s=12);else if(t===6)return iV(s,n);let l=Dr(a,Nn.MinusSign);return $i(s,n,l,i,r)}}function rV(t,n){switch(t){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new P(2301,!1)}}function at(t,n,e=en.Format,i=!1){return function(r,o){return oV(r,o,t,n,e,i)}}function oV(t,n,e,i,r,o){switch(e){case 2:return HM(n,r,i)[t.getMonth()];case 1:return BM(n,r,i)[t.getDay()];case 0:let a=t.getHours(),s=t.getMinutes();if(o){let c=GM(n),u=WM(n,r,i),m=c.findIndex(v=>{if(Array.isArray(v)){let[_,y]=v,I=a>=_.hours&&s>=_.minutes,R=a<y.hours||a===y.hours&&s<y.minutes;if(_.hours<y.hours){if(I&&R)return!0}else if(I||R)return!0}else if(v.hours===a&&v.minutes===s)return!0;return!1});if(m!==-1)return u[m]}return jM(n,r,i)[a<12?0:1];case 3:return UM(n,i)[t.getFullYear()<=0?0:1];default:let l=e;throw new P(2302,!1)}}function Nm(t){return function(n,e,i){let r=-1*i,o=Dr(e,Nn.MinusSign),a=r>0?Math.floor(r/60):Math.ceil(r/60);switch(t){case 0:return(r>=0?"+":"")+$i(a,2,o)+$i(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+$i(a,1,o);case 2:return"GMT"+(r>=0?"+":"")+$i(a,2,o)+":"+$i(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+$i(a,2,o)+":"+$i(Math.abs(r%60),2,o);default:throw new P(2310,!1)}}}var aV=0,Fm=4;function sV(t){let n=Lm(t,aV,1).getDay();return Lm(t,0,1+(n<=Fm?Fm:Fm+7)-n)}function ZM(t){let n=t.getDay(),e=n===0?-3:Fm-n;return Lm(t.getFullYear(),t.getMonth(),t.getDate()+e)}function Iy(t,n=!1){return function(e,i){let r;if(n){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,a=e.getDate();r=1+Math.floor((a+o)/7)}else{let o=ZM(e),a=sV(o.getFullYear()),s=o.getTime()-a.getTime();r=1+Math.round(s/6048e5)}return $i(r,t,Dr(i,Nn.MinusSign))}}function Pm(t,n=!1){return function(e,i){let o=ZM(e).getFullYear();return $i(o,t,Dr(i,Nn.MinusSign),n)}}var Ty={};function lV(t){if(Ty[t])return Ty[t];let n;switch(t){case"G":case"GG":case"GGG":n=at(3,Xe.Abbreviated);break;case"GGGG":n=at(3,Xe.Wide);break;case"GGGGG":n=at(3,Xe.Narrow);break;case"y":n=kt(0,1,0,!1,!0);break;case"yy":n=kt(0,2,0,!0,!0);break;case"yyy":n=kt(0,3,0,!1,!0);break;case"yyyy":n=kt(0,4,0,!1,!0);break;case"Y":n=Pm(1);break;case"YY":n=Pm(2,!0);break;case"YYY":n=Pm(3);break;case"YYYY":n=Pm(4);break;case"M":case"L":n=kt(1,1,1);break;case"MM":case"LL":n=kt(1,2,1);break;case"MMM":n=at(2,Xe.Abbreviated);break;case"MMMM":n=at(2,Xe.Wide);break;case"MMMMM":n=at(2,Xe.Narrow);break;case"LLL":n=at(2,Xe.Abbreviated,en.Standalone);break;case"LLLL":n=at(2,Xe.Wide,en.Standalone);break;case"LLLLL":n=at(2,Xe.Narrow,en.Standalone);break;case"w":n=Iy(1);break;case"ww":n=Iy(2);break;case"W":n=Iy(1,!0);break;case"d":n=kt(2,1);break;case"dd":n=kt(2,2);break;case"c":case"cc":n=kt(7,1);break;case"ccc":n=at(1,Xe.Abbreviated,en.Standalone);break;case"cccc":n=at(1,Xe.Wide,en.Standalone);break;case"ccccc":n=at(1,Xe.Narrow,en.Standalone);break;case"cccccc":n=at(1,Xe.Short,en.Standalone);break;case"E":case"EE":case"EEE":n=at(1,Xe.Abbreviated);break;case"EEEE":n=at(1,Xe.Wide);break;case"EEEEE":n=at(1,Xe.Narrow);break;case"EEEEEE":n=at(1,Xe.Short);break;case"a":case"aa":case"aaa":n=at(0,Xe.Abbreviated);break;case"aaaa":n=at(0,Xe.Wide);break;case"aaaaa":n=at(0,Xe.Narrow);break;case"b":case"bb":case"bbb":n=at(0,Xe.Abbreviated,en.Standalone,!0);break;case"bbbb":n=at(0,Xe.Wide,en.Standalone,!0);break;case"bbbbb":n=at(0,Xe.Narrow,en.Standalone,!0);break;case"B":case"BB":case"BBB":n=at(0,Xe.Abbreviated,en.Format,!0);break;case"BBBB":n=at(0,Xe.Wide,en.Format,!0);break;case"BBBBB":n=at(0,Xe.Narrow,en.Format,!0);break;case"h":n=kt(3,1,-12);break;case"hh":n=kt(3,2,-12);break;case"H":n=kt(3,1);break;case"HH":n=kt(3,2);break;case"m":n=kt(4,1);break;case"mm":n=kt(4,2);break;case"s":n=kt(5,1);break;case"ss":n=kt(5,2);break;case"S":n=kt(6,1);break;case"SS":n=kt(6,2);break;case"SSS":n=kt(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Nm(0);break;case"ZZZZZ":n=Nm(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Nm(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Nm(2);break;default:return null}return Ty[t]=n,n}function KM(t,n){t=t.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+t)/6e4;return isNaN(e)?n:e}function cV(t,n){return t=new Date(t.getTime()),t.setMinutes(t.getMinutes()+n),t}function dV(t,n,e){let r=t.getTimezoneOffset(),o=KM(n,r);return cV(t,-1*(o-r))}function uV(t){if(NM(t))return t;if(typeof t=="number"&&!isNaN(t))return new Date(t);if(typeof t=="string"){if(t=t.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(t)){let[r,o=1,a=1]=t.split("-").map(s=>+s);return Lm(r,o-1,a)}let e=parseFloat(t);if(!isNaN(t-e))return new Date(e);let i;if(i=t.match(tV))return fV(i)}let n=new Date(t);if(!NM(n))throw new P(2311,!1);return n}function fV(t){let n=new Date(0),e=0,i=0,r=t[8]?n.setUTCFullYear:n.setFullYear,o=t[8]?n.setUTCHours:n.setHours;t[9]&&(e=Number(t[9]+t[10]),i=Number(t[9]+t[11])),r.call(n,Number(t[1]),Number(t[2])-1,Number(t[3]));let a=Number(t[4]||0)-e,s=Number(t[5]||0)-i,l=Number(t[6]||0),c=Math.floor(parseFloat("0."+(t[7]||0))*1e3);return o.call(n,a,s,l,c),n}function NM(t){return t instanceof Date&&!isNaN(t.valueOf())}var mV=/^(\d+)?\.((\d+)(-(\d+))?)?$/,PM=22,Vm=".",Fc="0",pV=";",hV=",",Ay="#",FM="\xA4";function gV(t,n,e,i,r,o,a=!1){let s="",l=!1;if(!isFinite(t))s=Dr(e,Nn.Infinity);else{let c=yV(t);a&&(c=_V(c));let u=n.minInt,m=n.minFrac,v=n.maxFrac;if(o){let _e=o.match(mV);if(_e===null)throw new P(2306,!1);let yt=_e[1],vt=_e[3],ro=_e[5];yt!=null&&(u=Ry(yt)),vt!=null&&(m=Ry(vt)),ro!=null?v=Ry(ro):vt!=null&&m>v&&(v=m);let Rr=100;if(u>Rr||m>Rr||v>Rr)throw new P(2306,!1)}bV(c,m,v);let _=c.digits,y=c.integerLen,I=c.exponent,R=[];for(l=_.every(_e=>!_e);y<u;y++)_.unshift(0);for(;y<0;y++)_.unshift(0);y>0?R=_.splice(y,_.length):(R=_,_=[0]);let V=[];for(_.length>=n.lgSize&&V.unshift(_.splice(-n.lgSize,_.length).join(""));_.length>n.gSize;)V.unshift(_.splice(-n.gSize,_.length).join(""));_.length&&V.unshift(_.join("")),s=V.join(Dr(e,i)),R.length&&(s+=Dr(e,r)+R.join("")),I&&(s+=Dr(e,Nn.Exponential)+"+"+I)}return t<0&&!l?s=n.negPre+s+n.negSuf:s=n.posPre+s+n.posSuf,s}function XM(t,n,e,i,r){let o=zM(n,ky.Currency),a=vV(o,Dr(n,Nn.MinusSign));return a.minFrac=YM(i),a.maxFrac=a.minFrac,gV(t,a,n,Nn.CurrencyGroup,Nn.CurrencyDecimal,r).replace(FM,e).replace(FM,"").trim()}function vV(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(pV),r=i[0],o=i[1],a=r.indexOf(Vm)!==-1?r.split(Vm):[r.substring(0,r.lastIndexOf(Fc)+1),r.substring(r.lastIndexOf(Fc)+1)],s=a[0],l=a[1]||"";e.posPre=s.substring(0,s.indexOf(Ay));for(let u=0;u<l.length;u++){let m=l.charAt(u);m===Fc?e.minFrac=e.maxFrac=u+1:m===Ay?e.maxFrac=u+1:e.posSuf+=m}let c=s.split(hV);if(e.gSize=c[1]?c[1].length:0,e.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,m=o.indexOf(Ay);e.negPre=o.substring(0,m).replace(/'/g,""),e.negSuf=o.slice(m+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function _V(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function yV(t){let n=Math.abs(t)+"",e=0,i,r,o,a,s;for((r=n.indexOf(Vm))>-1&&(n=n.replace(Vm,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===Fc;o++);if(o===(s=n.length))i=[0],r=1;else{for(s--;n.charAt(s)===Fc;)s--;for(r-=o,i=[],a=0;o<=s;o++,a++)i[a]=Number(n.charAt(o))}return r>PM&&(i=i.splice(0,PM-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function bV(t,n,e){if(n>e)throw new P(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),a=o+t.integerLen,s=i[a];if(a>0){i.splice(Math.max(t.integerLen,a));for(let m=a;m<i.length;m++)i[m]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,a=o+1),i[0]=0;for(let m=1;m<a;m++)i[m]=0}if(s>=5)if(a-1<0){for(let m=0;m>a;m--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[a-1]++;for(;r<Math.max(0,o);r++)i.push(0);let l=o!==0,c=n+t.integerLen,u=i.reduceRight(function(m,v,_,y){return v=v+m,y[_]=v<10?v:v-10,l&&(y[_]===0&&_>=c?y.pop():l=!1),v>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function Ry(t){let n=parseInt(t);if(isNaN(n))throw new P(2305,!1);return n}var Bs=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(oe);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ee(ct))};static \u0275dir=M({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Pe]})}return t})();function JM(t,n){return new P(2100,!1)}var CV="mediumDate",eI=new C(""),tI=new C(""),Hs=(()=>{class t{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let a=i??this.defaultOptions?.dateFormat??CV,s=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return QM(e,a,o||this.locale,s)}catch(a){throw JM(t,a.message)}}static \u0275fac=function(i){return new(i||t)(ee(Vs,16),ee(eI,24),ee(tI,24))};static \u0275pipe=fm({name:"date",type:t,pure:!0})}return t})();var wr=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,a){if(!DV(e))return null;a||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let s=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?s=qM(s,r==="symbol"?"wide":"narrow",a):s=r);try{let l=wV(e);return XM(l,a,s,i,o)}catch(l){throw JM(t,l.message)}}static \u0275fac=function(i){return new(i||t)(ee(Vs,16),ee(cy,16))};static \u0275pipe=fm({name:"currency",type:t,pure:!0})}return t})();function DV(t){return!(t==null||t===""||t!==t)}function wV(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new P(2309,!1);return t}var Sa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})();function jm(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Ma=class{};var Oy="browser";function Bm(t){return t===Oy}var Lc=class{_doc;constructor(n){this._doc=n}manager},Hm=(()=>{class t extends Lc{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(se(j))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),$m=new C(""),Ly=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof Hm));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof Hm);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new P(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(se($m),se(re))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Ny="ng-app-id";function nI(t){for(let n of t)n.remove()}function iI(t,n){let e=n.createElement("style");return e.textContent=t,e}function xV(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Ny}="${n}"],link[${Ny}="${n}"]`);if(r)for(let o of r)o.removeAttribute(Ny),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function Fy(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Vy=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,xV(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,iI);i?.forEach(r=>this.addUsage(r,this.external,Fy))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(nI(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])nI(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,iI(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Fy(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(se(j),se(ui),se(Ea,8),se(jr))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Py={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},jy=/%COMP%/g;var oI="%COMP%",SV=`_nghost-${oI}`,MV=`_ngcontent-${oI}`,IV=!0,TV=new C("",{factory:()=>IV});function AV(t){return MV.replace(jy,t)}function RV(t){return SV.replace(jy,t)}function aI(t,n){return n.map(e=>e.replace(jy,t))}var By=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new Vc(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof zm?r.applyToHost(e):r instanceof jc&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,m=this.tracingService;switch(i.encapsulation){case Bi.Emulated:o=new zm(l,c,i,this.appId,u,a,s,m);break;case Bi.ShadowDom:return new Um(l,e,i,a,s,this.nonce,m,c);case Bi.ExperimentalIsolatedShadowDom:return new Um(l,e,i,a,s,this.nonce,m);default:o=new jc(l,c,i,u,a,s,m);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(se(Ly),se(Vy),se(ui),se(TV),se(j),se(re),se(Ea),se(Ui,8))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Vc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Py[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(rI(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(rI(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new P(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Py[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Py[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(_r.DashCase|_r.Important)?n.style.setProperty(e,i,r&_r.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&_r.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=pi().getGlobalEventTarget(this.doc,n),!n))throw new P(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function rI(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Um=class extends Vc{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=aI(i.id,c);for(let m of c){let v=document.createElement("style");a&&v.setAttribute("nonce",a),v.textContent=m,this.shadowRoot.appendChild(v)}let u=i.getExternalStyles?.();if(u)for(let m of u){let v=Fy(m,r);a&&v.setAttribute("nonce",a),this.shadowRoot.appendChild(v)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},jc=class extends Vc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?aI(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Ca.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},zm=class extends jc{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=AV(c),this.hostAttr=RV(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Gm=class t extends kc{supportsDOMEvents=!0;static makeCurrent(){Sy(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=kV();return e==null?null:OV(e)}resetBaseElement(){Bc=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return jm(document.cookie,n)}},Bc=null;function kV(){return Bc=Bc||document.head.querySelector("base"),Bc?Bc.getAttribute("href"):null}function OV(t){return new URL(t,document.baseURI).pathname}var NV=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),sI=["alt","control","meta","shift"],PV={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},FV={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},lI=(()=>{class t extends Lc{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>pi().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),sI.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=PV[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),sI.forEach(a=>{if(a!==r){let s=FV[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(se(j))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();async function Hy(t,n,e){let i=D({rootComponent:t},LV(n,e));return xM(i)}function LV(t,n){return{platformRef:n?.platformRef,appProviders:[...UV,...t?.providers??[]],platformProviders:HV}}function VV(){Gm.makeCurrent()}function jV(){return new ln}function BV(){return i_(document),document}var HV=[{provide:jr,useValue:Oy},{provide:$f,useValue:VV,multi:!0},{provide:j,useFactory:BV}];var UV=[{provide:ec,useValue:"root"},{provide:ln,useFactory:jV},{provide:$m,useClass:Hm,multi:!0},{provide:$m,useClass:lI,multi:!0},By,Vy,Ly,{provide:Tt,useExisting:By},{provide:Ma,useClass:NV},[]];var Gr=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=this.headers.get(e);if(!a)return;a=a.filter(s=>o.indexOf(s)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var zy=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},$y=class{encodeKey(n){return cI(n)}encodeValue(n){return cI(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function zV(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var $V=/%(\d[a-f0-9])/gi,GV={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function cI(t){return encodeURIComponent(t).replace($V,(n,e)=>GV[e]??n)}function Wm(t){return`${t}`}var $r=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new $y,n.fromString){if(n.fromObject)throw new P(2805,!1);this.map=zV(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Wm):[Wm(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Wm(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Wm(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function WV(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function dI(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function uI(t){return typeof Blob<"u"&&t instanceof Blob}function fI(t){return typeof FormData<"u"&&t instanceof FormData}function qV(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var mI="Content-Type",pI="Accept",hI="text/plain",gI="application/json",YV=`${gI}, ${hI}, */*`,zs=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(WV(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new P(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Gr,this.context??=new zy,!this.params)this.params=new $r,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||dI(this.body)||uI(this.body)||fI(this.body)||qV(this.body)?this.body:this.body instanceof $r?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||fI(this.body)?null:uI(this.body)?this.body.type||null:dI(this.body)?null:typeof this.body=="string"?hI:this.body instanceof $r?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?gI:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,m=n.referrer||this.referrer,v=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,y=n.transferCache??this.transferCache,I=n.timeout??this.timeout,R=n.body!==void 0?n.body:this.body,V=n.withCredentials??this.withCredentials,_e=n.reportProgress??this.reportProgress,yt=n.headers||this.headers,vt=n.params||this.params,ro=n.context??this.context;return n.setHeaders!==void 0&&(yt=Object.keys(n.setHeaders).reduce((Rr,Yo)=>Rr.set(Yo,n.setHeaders[Yo]),yt)),n.setParams&&(vt=Object.keys(n.setParams).reduce((Rr,Yo)=>Rr.set(Yo,n.setParams[Yo]),vt)),new t(e,i,R,{params:vt,headers:yt,context:ro,reportProgress:_e,responseType:r,withCredentials:V,transferCache:y,keepalive:o,cache:s,priority:a,timeout:I,mode:l,redirect:c,credentials:u,referrer:m,integrity:v,referrerPolicy:_})}},Ia=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Ia||{}),Hc=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Gr,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Gy=class t extends Hc{constructor(n={}){super(n)}type=Ia.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Gs=class t extends Hc{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Ia.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},$s=class extends Hc{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},QV=200,ZV=204;var KV=/^\)\]\}',?\n/;var XV=(()=>{class t{xhrFactory;tracingService=d(Ui,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new P(-2800,!1);let i=this.xhrFactory;return J(null).pipe(mt(()=>new ye(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((R,V)=>a.setRequestHeader(R,V.join(","))),e.headers.has(pI)||a.setRequestHeader(pI,YV),!e.headers.has(mI)){let R=e.detectContentTypeHeader();R!==null&&a.setRequestHeader(mI,R)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let R=e.responseType.toLowerCase();a.responseType=R!=="json"?R:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let R=a.statusText||"OK",V=new Gr(a.getAllResponseHeaders()),_e=a.responseURL||e.url;return l=new Gy({headers:V,status:a.status,statusText:R,url:_e}),l},u=this.maybePropagateTrace(()=>{let{headers:R,status:V,statusText:_e,url:yt}=c(),vt=null;V!==ZV&&(vt=typeof a.response>"u"?a.responseText:a.response),V===0&&(V=vt?QV:0);let ro=V>=200&&V<300;if(e.responseType==="json"&&typeof vt=="string"){let Rr=vt;vt=vt.replace(KV,"");try{vt=vt!==""?JSON.parse(vt):null}catch(Yo){vt=Rr,ro&&(ro=!1,vt={error:Yo,text:vt})}}ro?(o.next(new Gs({body:vt,headers:R,status:V,statusText:_e,url:yt||void 0})),o.complete()):o.error(new $s({error:vt,headers:R,status:V,statusText:_e,url:yt||void 0}))}),m=this.maybePropagateTrace(R=>{let{url:V}=c(),_e=new $s({error:R,status:a.status||0,statusText:a.statusText||"Unknown Error",url:V||void 0});o.error(_e)}),v=m;e.timeout&&(v=this.maybePropagateTrace(R=>{let{url:V}=c(),_e=new $s({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:V||void 0});o.error(_e)}));let _=!1,y=this.maybePropagateTrace(R=>{_||(o.next(c()),_=!0);let V={type:Ia.DownloadProgress,loaded:R.loaded};R.lengthComputable&&(V.total=R.total),e.responseType==="text"&&a.responseText&&(V.partialText=a.responseText),o.next(V)}),I=this.maybePropagateTrace(R=>{let V={type:Ia.UploadProgress,loaded:R.loaded};R.lengthComputable&&(V.total=R.total),o.next(V)});return a.addEventListener("load",u),a.addEventListener("error",m),a.addEventListener("timeout",v),a.addEventListener("abort",m),e.reportProgress&&(a.addEventListener("progress",y),s!==null&&a.upload&&a.upload.addEventListener("progress",I)),a.send(s),o.next({type:Ia.Sent}),()=>{a.removeEventListener("error",m),a.removeEventListener("abort",m),a.removeEventListener("load",u),a.removeEventListener("timeout",v),e.reportProgress&&(a.removeEventListener("progress",y),s!==null&&a.upload&&a.upload.removeEventListener("progress",I)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(se(Ma))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function JV(t,n){return n(t)}function ej(t,n,e){return(i,r)=>Bt(e,()=>n(i,o=>t(o,r)))}var tj=new C("",{factory:()=>[]}),Wy=new C(""),nj=new C("",{factory:()=>!0});var ij=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=se(XV),r},providedIn:"root"})}return t})();var rj=(()=>{class t{backend;injector;chain=null;pendingTasks=d(vs);contributeToStability=d(nj);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(tj),...this.injector.get(Wy,[])]));this.chain=i.reduceRight((r,o)=>ej(r,o,this.injector),JV)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(aa(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(se(ij),se(Ge))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),oj=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=se(rj),r},providedIn:"root"})}return t})();function Uy(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var qy=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof zs)o=e;else{let l;r.headers instanceof Gr?l=r.headers:l=new Gr(r.headers);let c;r.params&&(r.params instanceof $r?c=r.params:c=new $r({fromObject:r.params})),o=new zs(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=J(o).pipe(oa(l=>this.handler.handle(l)));if(e instanceof zs||r.observe==="events")return a;let s=a.pipe(Ie(l=>l instanceof Gs));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(he(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new P(2806,!1);return l.body}));case"blob":return s.pipe(he(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new P(2807,!1);return l.body}));case"text":return s.pipe(he(l=>{if(l.body!==null&&typeof l.body!="string")throw new P(2808,!1);return l.body}));default:return s.pipe(he(l=>l.body))}case"response":return s;default:throw new P(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new $r().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Uy(r,i))}post(e,i,r={}){return this.request("POST",e,Uy(r,i))}put(e,i,r={}){return this.request("PUT",e,Uy(r,i))}static \u0275fac=function(i){return new(i||t)(se(oj))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var aj=new C(""),sj="b",lj="h",cj="s",dj="st",uj="u",fj="rt",Yy=new C(""),mj=["GET","HEAD"];function yI(t,n){let a=n,{isCacheActive:e}=a,i=fC(a,["isCacheActive"]),{transferCache:r,method:o}=t;return!(!e||r===!1||t.withCredentials||o==="POST"&&!i.includePostRequests&&!r||o!=="POST"&&!mj.includes(o)||!i.includeRequestsWithAuthHeaders&&gj(t)||i.filter?.(t)===!1)}function bI(t,n){let{includeHeaders:e}=t,i=e;return typeof n=="object"&&n.includeHeaders&&(i=n.includeHeaders),i}function pj(t,n,e,i){let{transferCache:r}=t;if(!yI(t,n))return null;if(i)throw new P(2803,!1);let o=t.url,a=CI(t,o),s=e.get(a,null),l=bI(n,r);if(s){let{[sj]:c,[fj]:u,[lj]:m,[cj]:v,[dj]:_,[uj]:y}=s,I=c;switch(u){case"arraybuffer":I=_I(c);break;case"blob":I=new Blob([_I(c)]);break}let R=new Gr(m);return new Gs({body:I,headers:R,status:v,statusText:_,url:y})}return null}function hj(t,n){let e=d(Yy),i=d(Ms),r=d(aj,{optional:!0}),o=pj(t,e,i,r);if(o)return J(o);let{transferCache:a}=t,s=bI(e,a),l=t.url,c=CI(t,l);return yI(t,e),n(t)}function gj(t){return t.headers.has("authorization")||t.headers.has("proxy-authorization")||t.headers.has("cookie")}function vI(t){return[...t.keys()].sort().map(n=>`${n}=${t.getAll(n)}`).join("&")}function CI(t,n){let{params:e,method:i,responseType:r}=t,o=vI(e),a=t.serializeBody();a instanceof URLSearchParams?a=vI(a):typeof a!="string"&&(a="");let s=[i,r,n,a,o].join("|"),l=vj(s);return l}function vj(t){let n=0;for(let e of t)n=Math.imul(31,n)+e.charCodeAt(0)<<0;return n+=2147483648,n.toString()}function _I(t){let n=atob(t);return Uint8Array.from(n,i=>i.charCodeAt(0)).buffer}function DI(t){return[{provide:Yy,useFactory:()=>(fi("NgHttpTransferCache"),D({isCacheActive:!0},t))},{provide:Wy,useValue:hj,multi:!0},{provide:Mo,multi:!0,useFactory:()=>{let n=d(At),e=d(Yy);return()=>{n.whenStable().then(()=>{e.isCacheActive=!1})}}}]}var wI=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(se(j))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Uc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=se(yj),r},providedIn:"root"})}return t})(),yj=(()=>{class t extends Uc{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Ft.NONE:return i;case Ft.HTML:return Br(i,"HTML")?Hi(i):__(this._doc,String(i)).toString();case Ft.STYLE:return Br(i,"Style")?Hi(i):i;case Ft.SCRIPT:if(Br(i,"Script"))return Hi(i);throw new P(5200,!1);case Ft.URL:return Br(i,"URL")?Hi(i):wc(String(i));case Ft.RESOURCE_URL:if(Br(i,"ResourceURL"))return Hi(i);throw new P(5201,!1);default:throw new P(5202,!1)}}bypassSecurityTrustHtml(e){return m_(e)}bypassSecurityTrustStyle(e){return p_(e)}bypassSecurityTrustScript(e){return h_(e)}bypassSecurityTrustUrl(e){return g_(e)}bypassSecurityTrustResourceUrl(e){return v_(e)}static \u0275fac=function(i){return new(i||t)(se(j))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),qm=(function(t){return t[t.NoHttpTransferCache=0]="NoHttpTransferCache",t[t.HttpTransferCacheOptions=1]="HttpTransferCacheOptions",t[t.I18nSupport=2]="I18nSupport",t[t.EventReplay=3]="EventReplay",t[t.IncrementalHydration=4]="IncrementalHydration",t})(qm||{});function bj(t,n=[],e={}){return{\u0275kind:t,\u0275providers:n}}function EI(){return bj(qm.EventReplay,wM())}function xI(...t){let n=[],e=new Set;for(let{\u0275providers:r,\u0275kind:o}of t)e.add(o),r.length&&n.push(r);let i=e.has(qm.HttpTransferCacheOptions);return dr([[],[],EM(),e.has(qm.NoHttpTransferCache)||i?[]:DI({}),n])}var Ee="primary",id=Symbol("RouteTitle"),Jy=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Aa(t){return new Jy(t)}function Qy(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function OI(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return Qy(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!Qy(o,t.slice(0,o.length),s)||!Qy(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function Jm(t){return new Promise((n,e)=>{t.pipe(Nr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function Cj(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Er(t[e],n[e]))return!1;return!0}function Er(t,n){let e=t?eb(t):void 0,i=n?eb(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!NI(t[r],n[r]))return!1;return!0}function eb(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function NI(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function Dj(t){return t.length>0?t[t.length-1]:null}function Oa(t){return ia(t)?t:So(t)?st(Promise.resolve(t)):J(t)}function PI(t){return ia(t)?Jm(t):Promise.resolve(t)}var wj={exact:LI,subset:VI},FI={exact:Ej,subset:xj,ignored:()=>!0},pb={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Yc={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function hb(t,n,e){let i=t instanceof hn?t:n.parseUrl(t);return Rt(()=>tb(n.lastSuccessfulNavigation()?.finalUrl??new hn,i,D(D({},Yc),e)))}function tb(t,n,e){return wj[e.paths](t.root,n.root,e.matrixParams)&&FI[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function Ej(t,n){return Er(t,n)}function LI(t,n,e){if(!Ta(t.segments,n.segments)||!Zm(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!LI(t.children[i],n.children[i],e))return!1;return!0}function xj(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>NI(t[e],n[e]))}function VI(t,n,e){return jI(t,n,n.segments,e)}function jI(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Ta(r,e)||n.hasChildren()||!Zm(r,e,i))}else if(t.segments.length===e.length){if(!Ta(t.segments,e)||!Zm(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!VI(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Ta(t.segments,r)||!Zm(t.segments,r,i)||!t.children[Ee]?!1:jI(t.children[Ee],n,o,i)}}function Zm(t,n,e){return n.every((i,r)=>FI[e](t[r].parameters,i.parameters))}var hn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ze([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Aa(this.queryParams),this._queryParamMap}toString(){return Ij.serialize(this)}},ze=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Km(this)}},ko=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Aa(this.parameters),this._parameterMap}toString(){return HI(this)}};function Sj(t,n){return Ta(t,n)&&t.every((e,i)=>Er(e.parameters,n[i].parameters))}function Ta(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function Mj(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===Ee&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==Ee&&(e=e.concat(n(r,i)))}),e}var el=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new Oo,providedIn:"root"})}return t})(),Oo=class{parse(n){let e=new ib(n);return new hn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${zc(n.root,!0)}`,i=Rj(n.queryParams),r=typeof n.fragment=="string"?`#${Tj(n.fragment)}`:"";return`${e}${i}${r}`}},Ij=new Oo;function Km(t){return t.segments.map(n=>HI(n)).join("/")}function zc(t,n){if(!t.hasChildren())return Km(t);if(n){let e=t.children[Ee]?zc(t.children[Ee],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==Ee&&i.push(`${r}:${zc(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=Mj(t,(i,r)=>r===Ee?[zc(t.children[Ee],!1)]:[`${r}:${zc(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[Ee]!=null?`${Km(t)}/${e[0]}`:`${Km(t)}/(${e.join("//")})`}}function BI(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Ym(t){return BI(t).replace(/%3B/gi,";")}function Tj(t){return encodeURI(t)}function nb(t){return BI(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Xm(t){return decodeURIComponent(t)}function MI(t){return Xm(t.replace(/\+/g,"%20"))}function HI(t){return`${nb(t.path)}${Aj(t.parameters)}`}function Aj(t){return Object.entries(t).map(([n,e])=>`;${nb(n)}=${nb(e)}`).join("")}function Rj(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Ym(e)}=${Ym(r)}`).join("&"):`${Ym(e)}=${Ym(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var kj=/^[^\/()?;#]+/;function Zy(t){let n=t.match(kj);return n?n[0]:""}var Oj=/^[^\/()?;=#]+/;function Nj(t){let n=t.match(Oj);return n?n[0]:""}var Pj=/^[^=?&#]+/;function Fj(t){let n=t.match(Pj);return n?n[0]:""}var Lj=/^[^&#]+/;function Vj(t){let n=t.match(Lj);return n?n[0]:""}var ib=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ze([],{}):new ze([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new P(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[Ee]=new ze(e,i)),r}parseSegment(){let n=Zy(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new P(4009,!1);return this.capture(n),new ko(Xm(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=Nj(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Zy(this.remaining);r&&(i=r,this.capture(i))}n[Xm(e)]=Xm(i)}parseQueryParam(n){let e=Fj(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=Vj(this.remaining);a&&(i=a,this.capture(i))}let r=MI(e),o=MI(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Zy(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new P(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=Ee);let s=this.parseChildren(e+1);i[a??Ee]=Object.keys(s).length===1&&s[Ee]?s[Ee]:new ze([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new P(4011,!1)}};function UI(t){return t.segments.length>0?new ze([],{[Ee]:t}):t}function zI(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=zI(r);if(i===Ee&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new ze(t.segments,n);return jj(e)}function jj(t){if(t.numberOfChildren===1&&t.children[Ee]){let n=t.children[Ee];return new ze(t.segments.concat(n.segments),n.children)}return t}function No(t){return t instanceof hn}function $I(t,n,e=null,i=null,r=new Oo){let o=GI(t);return WI(o,n,e,i,r)}function GI(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new ze(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=UI(i);return n??r}function WI(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Ky(o,o,o,e,i,r);let a=Bj(n);if(a.toRoot())return Ky(o,o,new ze([],{}),e,i,r);let s=Hj(a,o,t),l=s.processChildren?Gc(s.segmentGroup,s.index,a.commands):YI(s.segmentGroup,s.index,a.commands);return Ky(o,s.segmentGroup,l,e,i,r)}function ep(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Qc(t){return typeof t=="object"&&t!=null&&t.outlets}function II(t,n,e){t||="\u0275";let i=new hn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Ky(t,n,e,i,r,o){let a={};for(let[c,u]of Object.entries(i??{}))a[c]=Array.isArray(u)?u.map(m=>II(c,m,o)):II(c,u,o);let s;t===n?s=e:s=qI(t,n,e);let l=UI(zI(s));return new hn(l,a,r)}function qI(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=qI(o,n,e)}),new ze(t.segments,i)}var tp=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&ep(i[0]))throw new P(4003,!1);let r=i.find(Qc);if(r&&r!==Dj(i))throw new P(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function Bj(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new tp(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new tp(e,n,i)}var qs=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function Hj(t,n,e){if(t.isAbsolute)return new qs(n,!0,0);if(!e)return new qs(n,!1,NaN);if(e.parent===null)return new qs(e,!0,0);let i=ep(t.commands[0])?0:1,r=e.segments.length-1+i;return Uj(e,r,t.numberOfDoubleDots)}function Uj(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new P(4005,!1);r=i.segments.length}return new qs(i,!1,r-o)}function zj(t){return Qc(t[0])?t[0].outlets:{[Ee]:t}}function YI(t,n,e){if(t??=new ze([],{}),t.segments.length===0&&t.hasChildren())return Gc(t,n,e);let i=$j(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new ze(t.segments.slice(0,i.pathIndex),{});return o.children[Ee]=new ze(t.segments.slice(i.pathIndex),t.children),Gc(o,0,r)}else return i.match&&r.length===0?new ze(t.segments,{}):i.match&&!t.hasChildren()?rb(t,n,e):i.match?Gc(t,0,r):rb(t,n,e)}function Gc(t,n,e){if(e.length===0)return new ze(t.segments,{});{let i=zj(e),r={};if(Object.keys(i).some(o=>o!==Ee)&&t.children[Ee]&&t.numberOfChildren===1&&t.children[Ee].segments.length===0){let o=Gc(t.children[Ee],n,e);return new ze(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=YI(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new ze(t.segments,r)}}function $j(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(Qc(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!AI(l,c,a))return o;i+=2}else{if(!AI(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function rb(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Qc(o)){let l=Gj(o.outlets);return new ze(i,l)}if(r===0&&ep(e[0])){let l=t.segments[n];i.push(new ko(l.path,TI(e[0]))),r++;continue}let a=Qc(o)?o.outlets[Ee]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&ep(s)?(i.push(new ko(a,TI(s))),r+=2):(i.push(new ko(a,{})),r++)}return new ze(i,{})}function Gj(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=rb(new ze([],{}),0,i))}),n}function TI(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function AI(t,n,e){return t==e.path&&Er(n,e.parameters)}var Wc="imperative",Ht=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Ht||{}),ei=class{id;url;constructor(n,e){this.id=n,this.url=e}},Ra=class extends ei{type=Ht.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Wi=class extends ei{urlAfterRedirects;type=Ht.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},tn=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(tn||{}),Zc=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Zc||{}),gi=class extends ei{reason;code;type=Ht.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function QI(t){return t instanceof gi&&(t.code===tn.Redirect||t.code===tn.SupersededByNewNavigation)}var qr=class extends ei{reason;code;type=Ht.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ka=class extends ei{error;target;type=Ht.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Kc=class extends ei{urlAfterRedirects;state;type=Ht.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},np=class extends ei{urlAfterRedirects;state;type=Ht.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ip=class extends ei{urlAfterRedirects;state;shouldActivate;type=Ht.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},rp=class extends ei{urlAfterRedirects;state;type=Ht.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},op=class extends ei{urlAfterRedirects;state;type=Ht.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ap=class{route;type=Ht.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},sp=class{route;type=Ht.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},lp=class{snapshot;type=Ht.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},cp=class{snapshot;type=Ht.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},dp=class{snapshot;type=Ht.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},up=class{snapshot;type=Ht.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Qs=class{},Xc=class{},Zs=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function Wj(t){return!(t instanceof Qs)&&!(t instanceof Zs)&&!(t instanceof Xc)}var fp=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new tl(this.rootInjector)}},tl=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new fp(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(se(Ge))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),mp=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=ob(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=ob(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=ab(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return ab(n,this._root).map(e=>e.value)}};function ob(t,n){if(t===n.value)return n;for(let e of n.children){let i=ob(t,e);if(i)return i}return null}function ab(t,n){if(t===n.value)return[n];for(let e of n.children){let i=ab(t,e);if(i.length)return i.unshift(n),i}return[]}var Jn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function Ws(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var Jc=class extends mp{snapshot;constructor(n,e){super(n),this.snapshot=e,vb(this,n)}toString(){return this.snapshot.toString()}};function ZI(t,n){let e=qj(t,n),i=new Ct([new ko("",{})]),r=new Ct({}),o=new Ct({}),a=new Ct({}),s=new Ct(""),l=new Yr(i,r,a,s,o,Ee,t,e.root);return l.snapshot=e.root,new Jc(new Jn(l,[]),e)}function qj(t,n){let e={},i={},r={},a=new Ks([],e,r,"",i,Ee,t,null,{},n);return new ed("",new Jn(a,[]))}var Yr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(he(c=>c[id]))??J(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(he(n=>Aa(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(he(n=>Aa(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function gb(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:D(D({},n.params),t.params),data:D(D({},n.data),t.data),resolve:D(D(D(D({},t.data),n.data),r?.data),t._resolvedData)}:i={params:D({},t.params),data:D({},t.data),resolve:D(D({},t.data),t._resolvedData??{})},r&&XI(r)&&(i.resolve[id]=r.title),i}var Ks=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[id]}constructor(n,e,i,r,o,a,s,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Aa(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Aa(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},ed=class extends mp{url;constructor(n,e){super(e),this.url=n,vb(this,e)}toString(){return KI(this._root)}};function vb(t,n){n.value._routerState=t,n.children.forEach(e=>vb(t,e))}function KI(t){let n=t.children.length>0?` { ${t.children.map(KI).join(", ")} } `:"";return`${t.value}${n}`}function Xy(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Er(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Er(n.params,e.params)||t.paramsSubject.next(e.params),Cj(n.url,e.url)||t.urlSubject.next(e.url),Er(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function sb(t,n){let e=Er(t.params,n.params)&&Sj(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||sb(t.parent,n.parent))}function XI(t){return typeof t.title=="string"||t.title===null}var JI=new C(""),Na=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Ee;activateEvents=new Z;deactivateEvents=new Z;attachEvents=new Z;detachEvents=new Z;routerOutletData=Tm();parentContexts=d(tl);location=d(ct);changeDetector=d(Fe);inputBinder=d(rd,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new P(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new P(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new P(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new P(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new lb(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Pe]})}return t})(),lb=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Yr?this.route:n===tl?this.childContexts:n===JI?this.outletData:this.parent.get(n,e)}},rd=new C(""),_b=(()=>{class t{outletDataSubscriptions=new Map;bindActivatedRouteToOutletComponent(e){this.unsubscribeFromRouteData(e),this.subscribeToRouteData(e)}unsubscribeFromRouteData(e){this.outletDataSubscriptions.get(e)?.unsubscribe(),this.outletDataSubscriptions.delete(e)}subscribeToRouteData(e){let{activatedRoute:i}=e,r=ar([i.queryParams,i.params,i.data]).pipe(mt(([o,a,s],l)=>(s=D(D(D({},o),a),s),l===0?J(s):Promise.resolve(s)))).subscribe(o=>{if(!e.isActivated||!e.activatedComponentRef||e.activatedRoute!==i||i.component===null){this.unsubscribeFromRouteData(e);return}let a=IM(i.component);if(!a){this.unsubscribeFromRouteData(e);return}for(let{templateName:s}of a.inputs)e.activatedComponentRef.setInput(s,o[s])});this.outletDataSubscriptions.set(e,r)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),yb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&z(0,"router-outlet")},dependencies:[Na],encapsulation:2})}return t})();function bb(t){let n=t.children&&t.children.map(bb),e=n?fe(D({},t),{children:n}):D({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==Ee&&(e.component=yb),e}function Yj(t,n,e){let i=td(t,n._root,e?e._root:void 0);return new Jc(i,n)}function td(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=Qj(t,n,e);return new Jn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>td(t,s)),a}}let i=Zj(n.value),r=n.children.map(o=>td(t,o));return new Jn(i,r)}}function Qj(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return td(t,i,r);return td(t,i)})}function Zj(t){return new Yr(new Ct(t.url),new Ct(t.params),new Ct(t.queryParams),new Ct(t.fragment),new Ct(t.data),t.outlet,t.component,t)}var Xs=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},eT="ngNavigationCancelingError";function pp(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=No(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=tT(!1,tn.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function tT(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[eT]=!0,e.cancellationCode=n,e}function Kj(t){return nT(t)&&No(t.url)}function nT(t){return!!t&&t[eT]}var cb=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Xy(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=Ws(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Ws(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Ws(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=Ws(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new up(o.value.snapshot))}),n.children.length&&this.forwardEvent(new cp(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Xy(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),Xy(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},hp=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Ys=class{component;route;constructor(n,e){this.component=n,this.route=e}};function Xj(t,n,e){let i=t._root,r=n?n._root:null;return $c(i,r,e,[i.value])}function Jj(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function nl(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!pg(t)?t:n.get(t):i}function $c(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Ws(n);return t.children.forEach(a=>{eB(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>qc(s,e.getContext(a),r)),r}function eB(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=tB(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new hp(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?$c(t,n,s?s.children:null,i,r):$c(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new Ys(s.outlet.component,a))}else a&&qc(n,s,r),r.canActivateChecks.push(new hp(i)),o.component?$c(t,null,s?s.children:null,i,r):$c(t,null,e,i,r);return r}function tB(t,n,e){if(typeof e=="function")return Bt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Ta(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Ta(t.url,n.url)||!Er(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!sb(t,n)||!Er(t.queryParams,n.queryParams);default:return!sb(t,n)}}function qc(t,n,e){let i=Ws(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?qc(a,n.children.getContext(o),e):qc(a,null,e):qc(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Ys(n.outlet.component,r)):e.canDeactivateChecks.push(new Ys(null,r)):e.canDeactivateChecks.push(new Ys(null,r))}function od(t){return typeof t=="function"}function nB(t){return typeof t=="boolean"}function iB(t){return t&&od(t.canLoad)}function rB(t){return t&&od(t.canActivate)}function oB(t){return t&&od(t.canActivateChild)}function aB(t){return t&&od(t.canDeactivate)}function sB(t){return t&&od(t.canMatch)}function iT(t){return t instanceof ra||t?.name==="EmptyError"}var Qm=Symbol("INITIAL_VALUE");function Js(){return mt(t=>ar(t.map(n=>n.pipe(lt(1),We(Qm)))).pipe(he(n=>{for(let e of n)if(e!==!0){if(e===Qm)return Qm;if(e===!1||lB(e))return e}return!0}),Ie(n=>n!==Qm),lt(1)))}function lB(t){return No(t)||t instanceof Xs}function rT(t){return t.aborted?J(void 0).pipe(lt(1)):new ye(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function oT(t){return Te(rT(t))}function cB(t){return Zt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?J(fe(D({},n),{guardsResult:!0})):dB(o,e,i).pipe(Zt(a=>a&&nB(a)?uB(e,r,t):J(a)),he(a=>fe(D({},n),{guardsResult:a})))})}function dB(t,n,e){return st(t).pipe(Zt(i=>gB(i.component,i.route,e,n)),Nr(i=>i!==!0,!0))}function uB(t,n,e){return st(n).pipe(oa(i=>po(mB(i.route.parent,e),fB(i.route,e),hB(t,i.path),pB(t,i.route))),Nr(i=>i!==!0,!0))}function fB(t,n){return t!==null&&n&&n(new dp(t)),J(!0)}function mB(t,n){return t!==null&&n&&n(new lp(t)),J(!0)}function pB(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return J(!0);let i=e.map(r=>Ai(()=>{let o=n._environmentInjector,a=nl(r,o),s=rB(a)?a.canActivate(n,t):Bt(o,()=>a(n,t));return Oa(s).pipe(Nr())}));return J(i).pipe(Js())}function hB(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>Jj(o)).filter(o=>o!==null).map(o=>Ai(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=nl(s,l),u=oB(c)?c.canActivateChild(e,t):Bt(l,()=>c(e,t));return Oa(u).pipe(Nr())});return J(a).pipe(Js())}));return J(r).pipe(Js())}function gB(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return J(!0);let o=r.map(a=>{let s=n._environmentInjector,l=nl(a,s),c=aB(l)?l.canDeactivate(t,n,e,i):Bt(s,()=>l(t,n,e,i));return Oa(c).pipe(Nr())});return J(o).pipe(Js())}function vB(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return J(!0);let a=o.map(s=>{let l=nl(s,t),c=iB(l)?l.canLoad(n,e):Bt(t,()=>l(n,e)),u=Oa(c);return r?u.pipe(oT(r)):u});return J(a).pipe(Js(),aT(i))}function aT(t){return $h(jt(n=>{if(typeof n!="boolean")throw pp(t,n)}),he(n=>n===!0))}function _B(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return J(!0);let s=a.map(l=>{let c=nl(l,t),u=sB(c)?c.canMatch(n,e,r):Bt(t,()=>c(n,e,r));return Oa(u).pipe(oT(o))});return J(s).pipe(Js(),aT(i))}var Wr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},nd=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function yB(t){throw new P(4e3,!1)}function bB(t){throw tT(!1,tn.GuardRejected)}var db=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Ee])throw yB(`${n.redirectTo}`);r=r.children[Ee]}}async applyRedirectCommands(n,e,i,r,o){let a=await CB(e,r,o);if(a instanceof hn)throw new nd(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new nd(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new hn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new ze(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new P(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function CB(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Jm(Oa(Bt(e,()=>i(n))))}function DB(t,n){return t.providers&&!t._injector&&(t._injector=ks(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Gi(t){return t.outlet||Ee}function wB(t,n){let e=t.filter(i=>Gi(i)===n);return e.push(...t.filter(i=>Gi(i)!==n)),e}var ub={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function sT(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function EB(t,n,e,i,r,o,a){let s=lT(t,n,e);if(!s.matched)return J(s);let l=sT(o(s));return i=DB(n,i),_B(i,n,e,r,l,a).pipe(he(c=>c===!0?s:D({},ub)))}function lT(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?D({},ub):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||OI)(e,t,n);if(!r)return D({},ub);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?D(D({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function RI(t,n,e,i,r){return e.length>0&&MB(t,e,i,r)?{segmentGroup:new ze(n,SB(i,new ze(e,t.children))),slicedSegments:[]}:e.length===0&&IB(t,e,i)?{segmentGroup:new ze(t.segments,xB(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new ze(t.segments,t.children),slicedSegments:e}}function xB(t,n,e,i){let r={};for(let o of e)if(vp(t,n,o)&&!i[Gi(o)]){let a=new ze([],{});r[Gi(o)]=a}return D(D({},i),r)}function SB(t,n){let e={};e[Ee]=n;for(let i of t)if(i.path===""&&Gi(i)!==Ee){let r=new ze([],{});e[Gi(i)]=r}return e}function MB(t,n,e,i){return e.some(r=>!vp(t,n,r)||!(Gi(r)!==Ee)?!1:!(i!==void 0&&Gi(r)===i))}function IB(t,n,e){return e.some(i=>vp(t,n,i))}function vp(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function TB(t,n,e){return n.length===0&&!t.children[e]}var fb=class{};async function AB(t,n,e,i,r,o,a="emptyOnly",s){return new mb(t,n,e,i,r,a,o,s).recognize()}var RB=31,mb=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new db(this.urlSerializer,this.urlTree)}noMatchError(n){return new P(4002,`'${n.segmentGroup}'`)}async recognize(){let n=RI(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Jn(i,e),o=new ed("",r),a=$I(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new Ks([],Object.freeze({}),Object.freeze(D({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Ee,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,Ee,e),rootSnapshot:e}}catch(i){if(i instanceof nd)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Wr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof Jn?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],u=wB(e,l),m=await this.processSegmentGroup(n,u,c,l,r);a.push(...m)}let s=cT(a);return kB(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof Wr||iT(c))continue;throw c}if(TB(i,r,o))return new fb;throw new Wr(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(Gi(i)!==a&&(a===Ee||!vp(r,o,i)))throw new Wr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new Wr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:m,remainingSegments:v}=lT(e,r,o);if(!l)throw new Wr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>RB&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let y=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,m,sT(_),n),I=await this.applyRedirects.lineralizeSegments(r,y);return this.processSegment(n,i,e,I.concat(v),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new Ks(i,r,Object.freeze(D({},this.urlTree.queryParams)),this.urlTree.fragment,NB(e),Gi(e),e.component??e._loadedComponent??null,e,PB(e),n),s=gb(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=yt=>this.createSnapshot(n,i,yt.consumedSegments,yt.parameters,a),l=await Jm(EB(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new Wr(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:m,consumedSegments:v,remainingSegments:_}=l,y=this.createSnapshot(n,i,v,m,a),{segmentGroup:I,slicedSegments:R}=RI(e,v,_,c,o);if(R.length===0&&I.hasChildren()){let yt=await this.processChildren(u,c,I,y);return new Jn(y,yt)}if(c.length===0&&R.length===0)return new Jn(y,[]);let V=Gi(i)===o,_e=await this.processSegment(u,c,I,R,V?Ee:o,!0,y);return new Jn(y,_e instanceof Jn?[_e]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Jm(vB(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw bB(e)}return{routes:[],injector:n}}};function kB(t){t.sort((n,e)=>n.value.outlet===Ee?-1:e.value.outlet===Ee?1:n.value.outlet.localeCompare(e.value.outlet))}function OB(t){let n=t.value.routeConfig;return n&&n.path===""}function cT(t){let n=[],e=new Set;for(let i of t){if(!OB(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=cT(i.children);n.push(new Jn(i.value,r))}return n.filter(i=>!e.has(i))}function NB(t){return t.data||{}}function PB(t){return t.resolve||{}}function FB(t,n,e,i,r,o,a){return Zt(async s=>{let{state:l,tree:c}=await AB(t,n,e,i,s.extractedUrl,r,o,a);return fe(D({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function LB(t){return Zt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return J(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of dT(s))o.add(l);let a=0;return st(o).pipe(oa(s=>r.has(s)?VB(s,e,t):(s.data=gb(s,s.parent,t).resolve,J(void 0))),jt(()=>a++),Ou(1),Zt(s=>a===o.size?J(n):ft))})}function dT(t){let n=t.children.map(e=>dT(e)).flat();return[t,...n]}function VB(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!XI(i)&&(r[id]=i.title),Ai(()=>(t.data=gb(t,t.parent,e).resolve,jB(r,t,n).pipe(he(o=>(t._resolvedData=o,t.data=D(D({},t.data),o),null)))))}function jB(t,n,e){let i=eb(t);if(i.length===0)return J({});let r={};return st(i).pipe(Zt(o=>BB(t[o],n,e).pipe(Nr(),jt(a=>{if(a instanceof Xs)throw pp(new Oo,a);r[o]=a}))),Ou(1),he(()=>r),ho(o=>iT(o)?ft:jl(o)))}function BB(t,n,e){let i=n._environmentInjector,r=nl(t,i),o=r.resolve?r.resolve(n,e):Bt(i,()=>r(n,e));return Oa(o)}function kI(t){return mt(n=>{let e=t(n);return e?st(e).pipe(he(()=>n)):J(n)})}var Cb=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===Ee);return i}getResolvedTitleForRoute(e){return e.data[id]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(uT),providedIn:"root"})}return t})(),uT=(()=>{class t extends Cb{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(se(wI))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),il=new C("",{factory:()=>({})}),ad=new C(""),fT=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(ly);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await PI(Bt(e,()=>i.loadComponent())),a=await hT(pT(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await mT(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function mT(t,n,e,i){let r=await PI(Bt(e,()=>t.loadChildren())),o=await hT(pT(r)),a;o instanceof um||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,u;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,u=a,l=s.get(ad,[],{optional:!0,self:!0}).flat()),{routes:l.map(bb),injector:s,factory:u}}function HB(t){return t&&typeof t=="object"&&"default"in t}function pT(t){return HB(t)?t.default:t}async function hT(t){return t}var _p=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(UB),providedIn:"root"})}return t})(),UB=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gT=new C("");var zB=()=>{},vT=new C(""),_T=(()=>{class t{currentNavigation=S(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=S(null);events=new k;transitionAbortWithErrorSubject=new k;configLoader=d(fT);environmentInjector=d(Ge);destroyRef=d(fn);urlSerializer=d(el);rootContexts=d(tl);location=d(Ro);inputBindingEnabled=d(rd,{optional:!0})!==null;titleStrategy=d(Cb);options=d(il,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(_p);createViewTransition=d(gT,{optional:!0});navigationErrorHandler=d(vT,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>J(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new ap(r)),i=r=>this.events.next(new sp(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;He(()=>{this.transitions?.next(fe(D({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Ct(null),this.transitions.pipe(Ie(i=>i!==null),mt(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return J(i).pipe(mt(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",tn.SupersededByNewNavigation),ft;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?fe(D({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new qr(s.id,this.urlSerializer.serialize(s.rawUrl),"",Zc.IgnoredSameUrlNavigation)),s.resolve(!1),ft;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return J(s).pipe(mt(m=>(this.events.next(new Ra(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?ft:Promise.resolve(m))),FB(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),jt(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(v=>(v.finalUrl=m.urlAfterRedirects,v)),this.events.next(new Xc)}),mt(m=>st(i.routesRecognizeHandler.deferredHandle??J(void 0)).pipe(he(()=>m))),jt(()=>{let m=new Kc(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(m)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:m,extractedUrl:v,source:_,restoredState:y,extras:I}=s,R=new Ra(m,this.urlSerializer.serialize(v),_,y);this.events.next(R);let V=ZI(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=fe(D({},s),{targetSnapshot:V,urlAfterRedirects:v,extras:fe(D({},I),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(_e=>(_e.finalUrl=v,_e)),J(i)}else return this.events.next(new qr(s.id,this.urlSerializer.serialize(s.extractedUrl),"",Zc.IgnoredByUrlHandlingStrategy)),s.resolve(!1),ft}),he(s=>{let l=new np(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=fe(D({},s),{guards:Xj(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),cB(s=>this.events.next(s)),mt(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw pp(this.urlSerializer,s.guardsResult);let l=new ip(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return ft;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",tn.GuardRejected),ft;if(s.guards.canActivateChecks.length===0)return J(s);let c=new rp(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return ft;let u=!1;return J(s).pipe(LB(this.paramsInheritanceStrategy),jt({next:()=>{u=!0;let m=new op(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(m)},complete:()=>{u||this.cancelNavigationTransition(s,"",tn.NoDataFromResolver)}}))}),kI(s=>{let l=u=>{let m=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let v=u._environmentInjector;m.push(this.configLoader.loadComponent(v,u.routeConfig).then(_=>{u.component=_}))}for(let v of u.children)m.push(...l(v));return m},c=l(s.targetSnapshot.root);return c.length===0?J(s):st(Promise.all(c).then(()=>s))}),kI(()=>this.afterPreactivation()),mt(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?st(c).pipe(he(()=>i)):J(i)}),lt(1),mt(s=>{let l=Yj(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=fe(D({},s),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new Qs);let c=i.beforeActivateHandler.deferredHandle;return c?st(c.then(()=>s)):J(s)}),jt(s=>{new cb(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=zB,l)),this.lastSuccessfulNavigation.set(He(this.currentNavigation)),this.events.next(new Wi(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),Te(rT(o.signal).pipe(Ie(()=>!r&&!i.targetRouterState),jt(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",tn.Aborted)}))),jt({complete:()=>{r=!0}}),Te(this.transitionAbortWithErrorSubject.pipe(jt(s=>{throw s}))),aa(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",tn.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),ho(s=>{if(r=!0,this.destroyed)return i.resolve(!1),ft;if(nT(s))this.events.next(new gi(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),Kj(s)?this.events.next(new Zs(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new ka(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=Bt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof Xs){let{message:u,cancellationCode:m}=pp(this.urlSerializer,c);this.events.next(new gi(i.id,this.urlSerializer.serialize(i.extractedUrl),u,m)),this.events.next(new Zs(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return ft}))}))}cancelNavigationTransition(e,i,r){let o=new gi(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=He(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function $B(t){return t!==Wc}var yT=new C("");var bT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(GB),providedIn:"root"})}return t})(),gp=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},GB=(()=>{class t extends gp{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),yp=(()=>{class t{urlSerializer=d(el);options=d(il,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Ro);urlHandlingStrategy=d(_p);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new hn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof hn?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=ZI(null,d(Ge));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(WB),providedIn:"root"})}return t})(),WB=(()=>{class t extends yp{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Ra?this.updateStateMemento():e instanceof qr?this.commitTransition(i):e instanceof Kc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Qs?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof gi&&!QI(e)?this.restoreHistory(i):e instanceof ka?this.restoreHistory(i,!0):e instanceof Wi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=D(D({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=D(D({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?D({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):D({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Db(t,n){t.events.pipe(Ie(e=>e instanceof Wi||e instanceof gi||e instanceof ka||e instanceof qr),he(e=>e instanceof Wi||e instanceof qr?0:(e instanceof gi?e.code===tn.Redirect||e.code===tn.SupersededByNewNavigation:!1)?2:1),Ie(e=>e!==2),lt(1)).subscribe(()=>{n()})}var gn=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(mm);stateManager=d(yp);options=d(il,{optional:!0})||{};pendingTasks=d(gr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(_T);urlSerializer=d(el);location=d(Ro);urlHandlingStrategy=d(_p);injector=d(Ge);_events=new k;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(bT);injectorCleanup=d(yT,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(ad,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(rd,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new pe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=He(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof gi&&i.code!==tn.Redirect&&i.code!==tn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Wi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Zs){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=D({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||$B(r.source)},a);this.scheduleNavigation(s,Wc,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}Wj(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Wc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=fe(D({},o),{browserUrl:e})),r){let c=D({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(Zn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return He(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(bb),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,u=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":u=D(D({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let m;try{let v=r?r.snapshot:this.routerState.snapshot.root;m=GI(v)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),m=this.currentUrlTree.root}return WI(m,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=No(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Wc,null,i)}navigate(e,i={skipLocationChange:!1}){return qB(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(lr(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=D({},pb):i===!1?r=D({},Yc):r=D(D({},Yc),i),No(e))return tb(this.currentUrlTree,e,r);let o=this.parseUrl(e);return tb(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((m,v)=>{s=m,l=v});let u=this.pendingTasks.add();return Db(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qB(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new P(4008,!1)}var QB=(()=>{class t{router=d(gn);stateManager=d(yp);fragment=S("");queryParams=S({});path=S("");serializer=d(el);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Wi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new hn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),rl=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new kn("href"),{optional:!0});reactiveHref=bm(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return He(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return He(this._target)}_target=S(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return He(this._queryParams)}_queryParams=S(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return He(this._fragment)}_fragment=S(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return He(this._queryParamsHandling)}_queryParamsHandling=S(void 0);set state(e){this._state.set(e)}get state(){return He(this._state)}_state=S(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return He(this._info)}_info=S(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return He(this._relativeTo)}_relativeTo=S(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return He(this._preserveFragment)}_preserveFragment=S(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return He(this._skipLocationChange)}_skipLocationChange=S(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return He(this._replaceUrl)}_replaceUrl=S(!1);isAnchorElement;onChanges=new k;applicationErrorHandler=d(Zn);options=d(il,{optional:!0});reactiveRouterState=d(QB);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=S(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(No(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Rt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:No(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return He(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(ee(gn),ee(Yr),yc("tabindex"),ee(qe),ee(W),ee(js))};static \u0275dir=M({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&F("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&de("href",r.reactiveHref(),C_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",q],skipLocationChange:[2,"skipLocationChange","skipLocationChange",q],replaceUrl:[2,"replaceUrl","replaceUrl",q],routerLink:"routerLink"},features:[Pe]})}return t})(),wb=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new Z;link=d(rl,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(a=>{a instanceof Wi&&this.update()})}ngAfterContentInit(){J(this.links.changes,J(null)).pipe(mo()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=st(e).pipe(mo()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=ZB(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?D({},pb):D({},Yc);return r=>{let o=r.urlTree;return o?He(hb(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(ee(gn),ee(W),ee(qe),ee(Fe))};static \u0275dir=M({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,rl,5),i&2){let a;Y(a=Q())&&(r.links=a)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[Pe]})}return t})();function ZB(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var KB=new C("");function Eb(t,...n){return dr([{provide:ad,multi:!0,useValue:t},[],{provide:Yr,useFactory:XB},{provide:Mo,multi:!0,useFactory:eH},n.map(e=>e.\u0275providers)])}function XB(){return d(gn).routerState.root}function JB(t,n){return{\u0275kind:t,\u0275providers:n}}function eH(){let t=d(oe);return n=>{let e=t.get(At);if(n!==e.components[0])return;let i=t.get(gn),r=t.get(tH);t.get(nH)===1&&i.initialNavigation(),t.get(iH,null,{optional:!0})?.setUpPreloading(),t.get(KB,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var tH=new C("",{factory:()=>new k}),nH=new C("",{factory:()=>1});var iH=new C("");function xb(){return JB(8,[_b,{provide:rd,useExisting:_b}])}var Mb;try{Mb=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Mb=!1}var ke=(()=>{class t{_platformId=d(jr);isBrowser=this._platformId?Bm(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Mb)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ib;function CT(){if(Ib==null){let t=typeof document<"u"?document.head:null;Ib=!!(t&&(t.createShadowRoot||t.attachShadow))}return Ib}function Tb(t){if(CT()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Pa(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Wt(t){return t.composedPath?t.composedPath()[0]:t.target}function Ab(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var bp=new WeakMap,_t=(()=>{class t{_appRef;_injector=d(oe);_environmentInjector=d(Ge);load(e){let i=this._appRef=this._appRef||this._injector.get(At),r=bp.get(i);r||(r={loaders:new Set,refs:[]},bp.set(i,r),i.onDestroy(()=>{bp.get(i)?.refs.forEach(o=>o.destroy()),bp.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Am(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function St(t){return t==null?"":typeof t=="string"?t:`${t}px`}function ol(t){return Array.isArray(t)?t:[t]}function xr(t,n=0){return DT(t)?Number(t):arguments.length===2?n:0}function DT(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function ti(t){return t instanceof W?t.nativeElement:t}var rH=new C("cdk-dir-doc",{providedIn:"root",factory:()=>d(j)}),oH=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function wT(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?oH.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Mt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=S("ltr");change=new Z;constructor(){let e=d(rH,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(wT(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var qi=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(qi||{}),Cp,Fa;function Dp(){if(Fa==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Fa=!1,Fa;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Fa=!0;else{let t=Element.prototype.scrollTo;t?Fa=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Fa=!1}}return Fa}function al(){if(typeof document!="object"||!document)return qi.NORMAL;if(Cp==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),Cp=qi.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,Cp=t.scrollLeft===0?qi.NEGATED:qi.INVERTED),t.remove()}return Cp}function wp(t){return t&&typeof t.connect=="function"&&!(t instanceof Nl)}var Yi=(function(t){return t[t.REPLACED=0]="REPLACED",t[t.INSERTED=1]="INSERTED",t[t.MOVED=2]="MOVED",t[t.REMOVED=3]="REMOVED",t})(Yi||{}),Ep=class{viewCacheSize=20;_viewCache=[];applyChanges(n,e,i,r,o){n.forEachOperation((a,s,l)=>{let c,u;if(a.previousIndex==null){let m=()=>i(a,s,l);c=this._insertView(m,l,e,r(a)),u=c?Yi.INSERTED:Yi.REPLACED}else l==null?(this._detachAndCacheView(s,e),u=Yi.REMOVED):(c=this._moveView(s,l,e,r(a)),u=Yi.MOVED);o&&o({context:c?.context,operation:u,record:a})})}detach(){for(let n of this._viewCache)n.destroy();this._viewCache=[]}_insertView(n,e,i,r){let o=this._insertViewFromCache(e,i);if(o){o.context.$implicit=r;return}let a=n();return i.createEmbeddedView(a.templateRef,a.context,a.index)}_detachAndCacheView(n,e){let i=e.detach(n);this._maybeCacheView(i,e)}_moveView(n,e,i,r){let o=i.get(n);return i.move(o,e),o.context.$implicit=r,o}_maybeCacheView(n,e){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(n);else{let i=e.indexOf(n);i===-1?n.destroy():e.remove(i)}}_insertViewFromCache(n,e){let i=this._viewCache.pop();return i&&e.insert(i,n),i||null}};var Ce=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})();var aH=20,sd=(()=>{class t{_ngZone=d(re);_platform=d(ke);_renderer=d(Tt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new k;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=aH){return this._platform.isBrowser?new ye(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(rs(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):J()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ie(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=ti(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ET=(()=>{class t{elementRef=d(W);scrollDispatcher=d(sd);ngZone=d(re);dir=d(Mt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new k;_renderer=d(qe);_cleanupScroll;_elementScrolled=new k;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&al()!=qi.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),al()==qi.INVERTED?e.left=e.right:al()==qi.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Dp()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&al()==qi.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&al()==qi.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),sH=20,Sr=(()=>{class t{_platform=d(ke);_listeners;_viewportSize=null;_change=new k;_document=d(j);constructor(){let e=d(re),i=d(Tt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=sH){return e>0?this._change.pipe(rs(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var xT=new C("CDK_VIRTUAL_SCROLL_VIEWPORT");var sl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})(),ld=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce,sl,Ce,sl]})}return t})();var Rb={},Ue=class t{_appId=d(ui);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),Rb.hasOwnProperty(n)||(Rb[n]=0),`${n}${e?t._infix+"-":""}${Rb[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var cd=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Qr=class extends cd{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},vn=class extends cd{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},kb=class extends cd{element;constructor(n){super(),this.element=n instanceof W?n.nativeElement:n}},Po=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Qr)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof vn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof kb)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},dd=class extends Po{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(yr,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||oe.NULL,o=r.get(Ge,i.injector);e=Am(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var vi=(()=>{class t extends Po{_moduleRef=d(yr,{optional:!0});_document=d(j);_viewContainerRef=d(ct);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new Z;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[ae]})}return t})(),_i=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})();function Ot(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var ST=Dp();function dl(t){return new xp(t.get(Sr),t.get(j))}var xp=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=St(-this._previousScrollPosition.left),n.style.top=St(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),ST&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),ST&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function OT(t,n){return new Sp(t.get(sd),t.get(re),t.get(Sr),n)}var Sp=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Ie(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var ud=class{enable(){}disable(){}attach(){}};function Ob(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function MT(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function ja(t,n){return new Mp(t.get(sd),t.get(Sr),t.get(re),n)}var Mp=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Ob(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},NT=(()=>{class t{_injector=d(oe);constructor(){}noop=()=>new ud;close=e=>OT(this._injector,e);block=()=>dl(this._injector);reposition=e=>ja(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Qi=class{positionStrategy;scrollStrategy=new ud;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Ip=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var PT=(()=>{class t{_attachedOverlays=[];_document=d(j);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),FT=(()=>{class t extends PT{_ngZone=d(re);_renderer=d(Tt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),LT=(()=>{class t extends PT{_platform=d(ke);_ngZone=d(re);_renderer=d(Tt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Wt(e)};_clickListener=e=>{let i=Wt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(IT(s.overlayElement,i)||IT(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function IT(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var VT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2,changeDetection:0})}return t})(),Rp=(()=>{class t{_platform=d(ke);_containerElement;_document=d(j);_styleLoader=d(_t);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Ab()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Ab()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(VT)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Nb=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Pb(t){return t&&t.nodeType===1}var ll=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new k;_attachments=new k;_detachments=new k;_positionStrategy;_scrollStrategy;_locationChanges=pe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new k;_outsidePointerEvents=new k;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,u=!1,m,v){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=m,this._renderer=v,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=ht(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=D(D({},this._config),n),this._updateElementSize()}setDirection(n){this._config=fe(D({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=St(this._config.width),n.height=St(this._config.height),n.minWidth=St(this._config.minWidth),n.minHeight=St(this._config.minHeight),n.maxWidth=St(this._config.maxWidth),n.maxHeight=St(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Pb(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Nb(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=ol(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=ht(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},TT="cdk-overlay-connected-position-bounding-box",cH=/([A-Za-z%]+)$/;function fd(t,n){return new Tp(n,t.get(Sr),t.get(j),t.get(ke),t.get(Rp))}var Tp=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new k;_resizeSubscription=pe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(TT),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),u=this._getOverlayFit(c,e,i,s);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<u.visibleArea)&&(a={overlayFit:u,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&La(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(TT),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof W?this._origin.nativeElement:Pb(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=RT(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let u=0-a,m=a+o.width-i.width,v=0-s,_=s+o.height-i.height,y=this._subtractOverflows(o.width,u,m),I=this._subtractOverflows(o.height,v,_),R=y*I;return{visibleArea:R,isCompletelyWithinViewport:o.width*o.height===R,fitsInViewportVertically:I===o.height,fitsInViewportHorizontally:y==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=AT(this._overlayRef.getConfig().minHeight),s=AT(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=RT(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,m=0;return r.width<=o.width?u=c||-a:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?m=l||-s:m=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:m},{x:n.x+u,y:n.y+m}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!dH(this._lastScrollVisibility,i)){let r=new Ip(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),y=this._lastBoundingBoxSize.height;o=_*2,a=n.y-_,o>y&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-y/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,m,v;if(c)v=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)m=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),y=this._lastBoundingBoxSize.width;u=_*2,m=n.x-_,u>y&&!this._isInitialRender&&!this._growAfterOpen&&(m=n.x-y/2)}return{top:a,left:m,bottom:s,right:v,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=St(i.width),r.height=St(i.height),r.top=St(i.top)||"auto",r.bottom=St(i.bottom)||"auto",r.left=St(i.left)||"auto",r.right=St(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=St(o)),a&&(r.maxWidth=St(a))}this._lastBoundingBoxSize=i,La(this._boundingBox.style,r)}_resetBoundingBoxStyles(){La(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){La(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();La(i,this._getExactOverlayY(e,n,u)),La(i,this._getExactOverlayX(e,n,u))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=St(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=St(a.maxWidth):o&&(i.maxWidth="")),La(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=St(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=St(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:MT(n,i),isOriginOutsideView:Ob(n,i),isOverlayClipped:MT(e,i),isOverlayOutsideView:Ob(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&ol(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof W)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function La(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function AT(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(cH);return!e||e==="px"?parseFloat(n):null}return t||null}function RT(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function dH(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var kT="cdk-global-overlay-wrapper";function Fo(t){return new Ap}var Ap=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(kT),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),u=this._xPosition,m=this._xOffset,v=this._overlayRef.getConfig().direction==="rtl",_="",y="",I="";l?I="flex-start":u==="center"?(I="center",v?y=m:_=m):v?u==="left"||u==="end"?(I="flex-end",_=m):(u==="right"||u==="start")&&(I="flex-start",y=m):u==="left"||u==="start"?(I="flex-start",_=m):(u==="right"||u==="end")&&(I="flex-end",y=m),n.position=this._cssPosition,n.marginLeft=l?"0":_,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":y,e.justifyContent=I,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(kT),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},jT=(()=>{class t{_injector=d(oe);constructor(){}global(){return Fo()}flexibleConnectedTo(e){return fd(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),md=new C("OVERLAY_DEFAULT_CONFIG");function Zr(t,n){t.get(_t).load(VT);let e=t.get(Rp),i=t.get(j),r=t.get(Ue),o=t.get(At),a=t.get(Mt),s=t.get(qe,null,{optional:!0})||t.get(Tt).createRenderer(null,null),l=new Qi(n),c=t.get(md,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),m=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),m.appendChild(u),l.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let v=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Pb(v)?v.after(m):v?.type==="parent"?v.element.appendChild(m):e.getContainerElement().appendChild(m),new ll(new dd(u,o,t),m,u,l,t.get(re),t.get(FT),i,t.get(Ro),t.get(LT),n?.disableAnimations??t.get(bc,null,{optional:!0})==="NoopAnimations",t.get(Ge),s)}var BT=(()=>{class t{scrollStrategies=d(NT);_positionBuilder=d(jT);_injector=d(oe);constructor(){}create(e){return Zr(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uH=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],fH=new C("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>ja(t)}}),cl=(()=>{class t{elementRef=d(W);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),HT=new C("cdk-connected-overlay-default-config"),kp=(()=>{class t{_dir=d(Mt,{optional:!0});_injector=d(oe);_overlayRef;_templatePortal;_backdropSubscription=pe.EMPTY;_attachSubscription=pe.EMPTY;_detachSubscription=pe.EMPTY;_positionSubscription=pe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(fH);_ngZone=d(re);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new Z;positionChange=new Z;attach=new Z;detach=new Z;overlayKeydown=new Z;overlayOutsideClick=new Z;constructor(){let e=d(Ve),i=d(ct),r=d(HT,{optional:!0}),o=d(md,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new vn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=uH);let e=this._overlayRef=Zr(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ot(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Wt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Qi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=fd(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof cl?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof cl?this.origin.elementRef.nativeElement:this.origin instanceof W?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Xh(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",q],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",q],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",q],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",q],push:[2,"cdkConnectedOverlayPush","push",q],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",q],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",q],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Pe]})}return t})(),Mr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({providers:[BT],imports:[Ce,_i,ld,ld]})}return t})();function Ba(t){return t.buttons===0||t.detail===0}function Ha(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var pd;function UT(){if(pd==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>pd=!0}))}finally{pd=pd||!1}return pd}function ul(t){return UT()?t:!!t.capture}var zT=new C("cdk-input-modality-detector-options"),$T={ignoreKeys:[18,17,224,91,16]},GT=650,Fb={passive:!0,capture:!0},WT=(()=>{class t{_platform=d(ke);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Ct(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Wt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<GT||(this._modality.next(Ba(e)?"keyboard":"mouse"),this._mostRecentTarget=Wt(e))};_onTouchstart=e=>{if(Ha(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Wt(e)};constructor(){let e=d(re),i=d(j),r=d(zT,{optional:!0});if(this._options=D(D({},$T),r),this.modalityDetected=this._modality.pipe(zl(1)),this.modalityChanged=this.modalityDetected.pipe(ku()),this._platform.isBrowser){let o=d(Tt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Fb),o.listen(i,"mousedown",this._onMousedown,Fb),o.listen(i,"touchstart",this._onTouchstart,Fb)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),hd=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(hd||{}),qT=new C("cdk-focus-monitor-default-options"),Op=ul({passive:!0,capture:!0}),Pn=(()=>{class t{_ngZone=d(re);_platform=d(ke);_inputModalityDetector=d(WT);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(j);_stopInputModalityDetector=new k;constructor(){let e=d(qT,{optional:!0});this._detectionMode=e?.detectionMode||hd.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Wt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=ti(e);if(!this._platform.isBrowser||r.nodeType!==1)return J();let o=Tb(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new k,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=ti(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=ti(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===hd.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===hd.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?GT:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Wt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Op),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Op)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Te(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Op),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Op),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var fl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2,changeDetection:0})}return t})(),Np;function mH(){if(Np===void 0&&(Np=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Np=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Np}function Ua(t){return mH()?.createHTML(t)||t}function YT(t,n,e){let i=e.sanitize(Ft.HTML,n);t.innerHTML=Ua(i||"")}var QT=new Set,za,Pp=(()=>{class t{_platform=d(ke);_nonce=d(Ea,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):hH}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&pH(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function pH(t,n){if(!QT.has(t))try{za||(za=document.createElement("style"),n&&za.setAttribute("nonce",n),za.setAttribute("type","text/css"),document.head.appendChild(za)),za.sheet&&(za.sheet.insertRule(`@media ${t} {body{ }}`,0),QT.add(t))}catch(e){console.error(e)}}function hH(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var gd=(()=>{class t{_mediaMatcher=d(Pp);_zone=d(re);_queries=new Map;_destroySubject=new k;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return ZT(ol(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=ZT(ol(e)).map(a=>this._registerQuery(a).observable),o=ar(r);return o=po(o.pipe(lt(1)),o.pipe(zl(1),Hl(0))),o.pipe(he(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ye(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(We(i),he(({matches:a})=>({query:e,matches:a})),Te(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ZT(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var gH=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Fp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({providers:[gH]})}return t})();var jb=(()=>{class t{_platform=d(ke);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return _H(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=vH(SH(e));if(i&&(KT(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=KT(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!EH(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return xH(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function vH(t){try{return t.frameElement}catch{return null}}function _H(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function yH(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function bH(t){return DH(t)&&t.type=="hidden"}function CH(t){return wH(t)&&t.hasAttribute("href")}function DH(t){return t.nodeName.toLowerCase()=="input"}function wH(t){return t.nodeName.toLowerCase()=="a"}function eA(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function KT(t){if(!eA(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function EH(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function xH(t){return bH(t)?!1:yH(t)||CH(t)||t.hasAttribute("contenteditable")||eA(t)}function SH(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Vb=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?ht(n,{injector:this._injector}):setTimeout(n)}},Bb=(()=>{class t{_checker=d(jb);_ngZone=d(re);_document=d(j);_injector=d(oe);constructor(){d(_t).load(fl)}create(e,i=!1){return new Vb(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var tA=new C("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),nA=new C("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),MH=0,vd=(()=>{class t{_ngZone=d(re);_defaultOptions=d(nA,{optional:!0});_liveElement;_document=d(j);_sanitizer=d(Uc);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(tA,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:YT(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${MH++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Lo=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(Lo||{}),XT="cdk-high-contrast-black-on-white",JT="cdk-high-contrast-white-on-black",Lb="cdk-high-contrast-active",iA=(()=>{class t{_platform=d(ke);_hasCheckedHighContrastMode=!1;_document=d(j);_breakpointSubscription;constructor(){this._breakpointSubscription=d(gd).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return Lo.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return Lo.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return Lo.BLACK_ON_WHITE}return Lo.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(Lb,XT,JT),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===Lo.BLACK_ON_WHITE?e.add(Lb,XT):i===Lo.WHITE_ON_BLACK&&e.add(Lb,JT)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Hb=(()=>{class t{constructor(){d(iA)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Fp]})}return t})();function IH(t,n){}var Vo=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var zb=(()=>{class t extends Po{_elementRef=d(W);_focusTrapFactory=d(Bb);_config;_interactivityChecker=d(jb);_ngZone=d(re);_focusMonitor=d(Pn);_renderer=d(qe);_changeDetectorRef=d(Fe);_injector=d(oe);_platform=d(ke);_document=d(j);_portalOutlet;_focusTrapped=new k;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(Vo,{optional:!0})||new Vo,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||ht(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Pa(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Pa();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Pa()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&dt(vi,7),i&2){let o;Y(o=Q())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&de("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[ae],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&N(0,IH,0,0,"ng-template",0)},dependencies:[vi],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),_d=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new k;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Ot(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},TH=new C("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>dl(t)}}),AH=new C("DialogData"),RH=new C("DefaultDialogConfig");function kH(t){let n=S(t),e=new Z;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var $b=(()=>{class t{_injector=d(oe);_defaultOptions=d(RH,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Rp);_idGenerator=d(Ue);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new k;_afterOpenedAtThisLevel=new k;_ariaHiddenElements=new Map;_scrollStrategy=d(TH);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=Ai(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(We(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new Vo;i=D(D({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=Zr(this._injector,o),s=new _d(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(lt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){Ub(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){Ub(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),Ub(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Qi({positionStrategy:e.positionStrategy||Fo().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:Vo,useValue:r},{provide:_d,useValue:i},{provide:ll,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=zb;let l=new Qr(s,r.viewContainerRef,oe.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Ve){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=D(D({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new vn(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new Qr(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:AH,useValue:e.data},{provide:_d,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(Mt,null,{optional:!0}))&&s.push({provide:Mt,useValue:kH(e.direction)}),oe.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ub(t,n){let e=t.length;for(;e--;)n(t[e])}var rA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({providers:[$b],imports:[Mr,_i,Hb,_i]})}return t})();function Kr(t){return t!=null&&`${t}`!="false"}var oA={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var OH=new C("MATERIAL_ANIMATIONS"),aA=null;function NH(){return d(OH,{optional:!0})?.animationsDisabled||d(bc,{optional:!0})==="NoopAnimations"?"di-disabled":(aA??=d(Pp).matchMedia("(prefers-reduced-motion)").matches,aA?"reduced-motion":"enabled")}function tt(){return NH()!=="enabled"}var PH=200,Lp=class{_letterKeyStream=new k;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new k;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:PH;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(jt(e=>this._pressedLetters.push(e)),Hl(n),Ie(()=>this._pressedLetters.length>0),he(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var ml=class{_items;_activeItemIndex=S(-1);_activeItem=S(null);_wrap=!1;_typeaheadSubscription=pe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Xt?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):br(n)&&(this._effectRef=Li(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new k;change=new k;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Lp(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||Ot(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return br(this._items)?this._items():this._items instanceof Xt?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var wd=class extends ml{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Xr=class extends ml{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var lA=" ";function cA(t,n,e){let i=dA(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(lA)))}function Gb(t,n,e){let i=dA(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(lA)):t.removeAttribute(n)}function dA(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}function FH(t,n){}var pl=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Wb="mdc-dialog--open",uA="mdc-dialog--opening",fA="mdc-dialog--closing",LH=150,VH=75,jH=(()=>{class t extends zb{_animationStateChanged=new Z;_animationsEnabled=!tt();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?pA(this._config.enterAnimationDuration)??LH:0;_exitAnimationDuration=this._animationsEnabled?pA(this._config.exitAnimationDuration)??VH:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(mA,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(uA,Wb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Wb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Wb),this._animationsEnabled?(this._hostElement.style.setProperty(mA,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(fA)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(uA,fA)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(xt("id",r._config.id),de("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),X("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[ae],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",0)(1,"div",1),N(2,FH,0,0,"ng-template",2),p()())},dependencies:[vi],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2})}return t})(),mA="--mat-dialog-transition-duration";function pA(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?xr(t.substring(0,t.length-2)):t.endsWith("s")?xr(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Vp=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Vp||{}),Vt=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new co(1);_beforeClosed=new co(1);_result;_closeFallbackTimeout;_state=Vp.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(Ie(r=>r.state==="opened"),lt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ie(r=>r.state==="closed"),lt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),an(this.backdropClick(),this.keydownEvents().pipe(Ie(r=>r.keyCode===27&&!this.disableClose&&!Ot(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),BH(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(Ie(i=>i.state==="closing"),lt(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Vp.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Vp.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function BH(t,n,e){return t._closeInteractionType=n,t.close(e)}var yi=new C("MatMdcDialogData"),qb=new C("mat-mdc-dialog-default-options"),HH=new C("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>dl(t)}}),Zi=(()=>{class t{_defaultOptions=d(qb,{optional:!0});_scrollStrategy=d(HH);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ue);_injector=d(oe);_dialog=d($b);_animationsDisabled=tt();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new k;_afterOpenedAtThisLevel=new k;dialogConfigClass=pl;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=Ai(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(We(void 0)));constructor(){this._dialogRefConstructor=Vt,this._dialogContainerType=jH,this._dialogDataToken=yi}open(e,i){let r;i=D(D({},this._defaultOptions||new pl),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,fe(D({},i),{positionStrategy:Fo(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:Vo,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var nn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Q_([ET])]})}return t})();var Yb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({providers:[Zi],imports:[rA,Mr,_i,Ce]})}return t})();var hA=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=Kr(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=Kr(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(de("aria-orientation",r.vertical?"vertical":"horizontal"),X("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
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
`],encapsulation:2,changeDetection:0})}return t})(),Jr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var hl,gA=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Qb(){if(hl)return hl;if(typeof document!="object"||!document)return hl=new Set(gA),hl;let t=document.createElement("input");return hl=new Set(gA.filter(n=>(t.setAttribute("type",n),t.type===n))),hl}var bi=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(bi||{}),Zb=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=bi.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},vA=ul({passive:!0,capture:!0}),Kb=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,vA)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,vA)))}_delegateEventHandler=n=>{let e=Wt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},xd={enterDuration:225,exitDuration:150},zH=800,_A=ul({passive:!0,capture:!0}),yA=["mousedown","touchstart"],bA=["mouseup","mouseleave","touchend","touchcancel"],$H=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2,changeDetection:0})}return t})(),Sd=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Kb;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=ti(i)),o&&o.get(_t).load($H)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=D(D({},xd),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||GH(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${s-a}px`,u.style.top=`${l-a}px`,u.style.height=`${a*2}px`,u.style.width=`${a*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let m=window.getComputedStyle(u),v=m.transitionProperty,_=m.transitionDuration,y=v==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,I=new Zb(this,u,i,y);u.style.transform="scale3d(1, 1, 1)",I.state=bi.FADING_IN,i.persistent||(this._mostRecentTransientRipple=I);let R=null;return!y&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let V=()=>{R&&(R.fallbackTimer=null),clearTimeout(yt),this._finishRippleTransition(I)},_e=()=>this._destroyRipple(I),yt=setTimeout(_e,c+100);u.addEventListener("transitionend",V),u.addEventListener("transitioncancel",_e),R={onTransitionEnd:V,onTransitionCancel:_e,fallbackTimer:yt}}),this._activeRipples.set(I,R),(y||!c)&&this._finishRippleTransition(I),I}fadeOutRipple(n){if(n.state===bi.FADING_OUT||n.state===bi.HIDDEN)return;let e=n.element,i=D(D({},xd),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=bi.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=ti(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,yA.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{bA.forEach(e=>{this._triggerElement.addEventListener(e,this,_A)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===bi.FADING_IN?this._startFadeOutTransition(n):n.state===bi.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=bi.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=bi.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Ba(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+zH;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Ha(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===bi.VISIBLE||n.config.terminateOnPointerUp&&n.state===bi.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(yA.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(bA.forEach(e=>n.removeEventListener(e,this,_A)),this._pointerUpEventsRegistered=!1))}};function GH(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Xb=new C("mat-ripple-global-options"),jo=(()=>{class t{_elementRef=d(W);_animationsDisabled=tt();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(re),i=d(ke),r=d(Xb,{optional:!0}),o=d(oe);this._globalOptions=r||{},this._rippleRenderer=new Sd(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:D(D(D({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,D(D({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,D(D({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&X("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var WH={capture:!0},qH=["focus","mousedown","mouseenter","touchstart"],Jb="mat-ripple-loader-uninitialized",e0="mat-ripple-loader-class-name",CA="mat-ripple-loader-centered",jp="mat-ripple-loader-disabled",DA=(()=>{class t{_document=d(j);_animationsDisabled=tt();_globalRippleOptions=d(Xb,{optional:!0});_platform=d(ke);_ngZone=d(re);_injector=d(oe);_eventCleanups;_hosts=new Map;constructor(){let e=d(Tt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>qH.map(i=>e.listen(this._document,i,this._onInteraction,WH)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Jb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(e0))&&e.setAttribute(e0,i.className||""),i.centered&&e.setAttribute(CA,""),i.disabled&&e.setAttribute(jp,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(jp,""):e.removeAttribute(jp)}_onInteraction=e=>{let i=Wt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Jb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(e0)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??xd.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??xd.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(jp),rippleConfig:{centered:e.hasAttribute(CA),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Sd(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(Jb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ci=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2,changeDetection:0})}return t})();var YH=["mat-icon-button",""],QH=["*"],ZH=new C("MAT_BUTTON_CONFIG");function wA(t){return t==null?void 0:zi(t)}var Bp=(()=>{class t{_elementRef=d(W);_ngZone=d(re);_animationsDisabled=tt();_config=d(ZH,{optional:!0});_focusMonitor=d(Pn);_cleanupClick;_renderer=d(qe);_rippleLoader=d(DA);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(_t).load(Ci);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(de("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),$t(r.color?"mat-"+r.color:""),X("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",q],disabled:[2,"disabled","disabled",q],ariaDisabled:[2,"aria-disabled","ariaDisabled",q],disabledInteractive:[2,"disabledInteractive","disabledInteractive",q],tabIndex:[2,"tabIndex","tabIndex",wA],_tabindex:[2,"tabindex","_tabindex",wA]}})}return t})(),eo=(()=>{class t extends Bp{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[ae],attrs:YH,ngContentSelectors:QH,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(),Jt(0,"span",0),K(1),Jt(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2,changeDetection:0})}return t})();var Bo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var KH=["matButton",""],xA=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],SA=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var XH=["mat-mini-fab",""],JH=`.mat-mdc-fab-base {
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
`,EA=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),ut=(()=>{class t extends Bp{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=eU(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?EA.get(this._appearance):null,o=EA.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[ae],attrs:KH,ngContentSelectors:SA,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(xA),Jt(0,"span",0),K(1),Et(2,"span",1),K(3,1),Lt(),K(4,2),Jt(5,"span",2)(6,"span",3)),i&2&&X("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2,changeDetection:0})}return t})();function eU(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var tU=new C("mat-mdc-fab-default-options",{providedIn:"root",factory:()=>t0}),t0={color:"accent"};var Hp=(()=>{class t extends Bp{_options=d(tU,{optional:!0});_isFab=!0;constructor(){super(),this._options=this._options||t0,this.color=this._options.color||t0.color}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-mini-fab",""],["a","mat-mini-fab",""],["button","matMiniFab",""],["a","matMiniFab",""]],hostAttrs:[1,"mdc-fab","mat-mdc-fab-base","mdc-fab--mini","mat-mdc-mini-fab"],exportAs:["matButton","matAnchor"],features:[ae],attrs:XH,ngContentSelectors:SA,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Me(xA),Jt(0,"span",0),K(1),Et(2,"span",1),K(3,1),Lt(),K(4,2),Jt(5,"span",2)(6,"span",3)),i&2&&X("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[JH],encapsulation:2,changeDetection:0})}return t})();var Ho=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Bo,Ce]})}return t})();function nU(t,n){if(t&1){let e=Ke();f(0,"div",1)(1,"button",2),F("click",function(){Ae(e);let r=b();return Re(r.action())}),g(2),p()()}if(t&2){let e=b();h(2),$(" ",e.data.action," ")}}var iU=["label"];function rU(t,n){}var oU=Math.pow(2,31)-1,Md=class{_overlayRef;instance;containerInstance;_afterDismissed=new k;_afterOpened=new k;_onAction=new k;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,oU))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},MA=new C("MatSnackBarData"),Uo=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},aU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),sU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),lU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),cU=(()=>{class t{snackBarRef=d(Md);data=d(MA);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(f(0,"div",0),g(1),p(),T(2,nU,3,1,"div",1)),i&2&&(h(),$(" ",r.data.message,`
`),h(),A(r.hasAction?2:-1))},dependencies:[ut,aU,sU,lU],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),n0="_mat-snack-bar-enter",i0="_mat-snack-bar-exit",dU=(()=>{class t extends Po{_ngZone=d(re);_elementRef=d(W);_changeDetectorRef=d(Fe);_platform=d(ke);_animationsDisabled=tt();snackBarConfig=d(Uo);_document=d(j);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(oe);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new k;_onExit=new k;_onEnter=new k;_animationState="void";_live;_label;_role;_liveElementId=d(Ue).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===i0?this._completeExit():e===n0&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?ht(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(n0)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(n0)},200)))}exit(){return this._destroyed?J(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?ht(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(i0)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(i0),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&dt(vi,7)(iU,7),i&2){let o;Y(o=Q())&&(r._portalOutlet=o.first),Y(o=Q())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&F("animationend",function(a){return r.onAnimationEnd(a.animationName)})("animationcancel",function(a){return r.onAnimationEnd(a.animationName)}),i&2&&X("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[ae],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2,0)(3,"div",3),N(4,rU,0,0,"ng-template",4),p(),z(5,"div"),p()()),i&2&&(h(5),de("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[vi],styles:[`@keyframes _mat-snack-bar-enter {
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
`],encapsulation:2})}return t})(),uU=new C("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Uo}),qt=(()=>{class t{_live=d(vd);_injector=d(oe);_breakpointObserver=d(gd);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(uU);_animationsDisabled=tt();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=cU;snackBarContainerComponent=dU;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=D(D({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=oe.create({parent:r||this._injector,providers:[{provide:Uo,useValue:i}]}),a=new Qr(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=D(D(D({},new Uo),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new Md(a,o);if(e instanceof Ve){let l=new vn(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(l)}else{let l=this._createInjector(r,s),c=new Qr(e,void 0,l),u=a.attachComponentPortal(c);s.instance=u.instance}return this._breakpointObserver.observe(oA.HandsetPortrait).pipe(Te(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Qi;i.direction=e.direction;let r=Fo(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Zr(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return oe.create({parent:r||this._injector,providers:[{provide:Md,useValue:i},{provide:MA,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var FA=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ee(qe),ee(W))};static \u0275dir=M({type:t})}return t})(),fU=(()=>{class t extends FA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,features:[ae]})}return t})(),Pd=new C("");var mU={provide:Pd,useExisting:Rn(()=>Fn),multi:!0};function pU(){let t=pi()?pi().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var hU=new C(""),Fn=(()=>{class t extends FA{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!pU())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ee(qe),ee(W),ee(hU,8))};static \u0275dir=M({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&F("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[we([mU]),ae]})}return t})();function s0(t){return t==null||l0(t)===0}function l0(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Ga=new C(""),Xp=new C(""),gU=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,H=class{static min(n){return vU(n)}static max(n){return _U(n)}static required(n){return LA(n)}static requiredTrue(n){return yU(n)}static email(n){return bU(n)}static minLength(n){return CU(n)}static maxLength(n){return DU(n)}static pattern(n){return wU(n)}static nullValidator(n){return zp()}static compose(n){return zA(n)}static composeAsync(n){return $A(n)}};function vU(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function _U(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function LA(t){return s0(t.value)?{required:!0}:null}function yU(t){return t.value===!0?null:{required:!0}}function bU(t){return s0(t.value)||gU.test(t.value)?null:{email:!0}}function CU(t){return n=>{let e=n.value?.length??l0(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function DU(t){return n=>{let e=n.value?.length??l0(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function wU(t){if(!t)return zp;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(s0(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function zp(t){return null}function VA(t){return t!=null}function jA(t){return So(t)?st(t):t}function BA(t){let n={};return t.forEach(e=>{n=e!=null?D(D({},n),e):n}),Object.keys(n).length===0?null:n}function HA(t,n){return n.map(e=>e(t))}function EU(t){return!t.validate}function UA(t){return t.map(n=>EU(n)?n:e=>n.validate(e))}function zA(t){if(!t)return null;let n=t.filter(VA);return n.length==0?null:function(e){return BA(HA(e,n))}}function c0(t){return t!=null?zA(UA(t)):null}function $A(t){if(!t)return null;let n=t.filter(VA);return n.length==0?null:function(e){let i=HA(e,n).map(jA);return Bl(i).pipe(he(BA))}}function d0(t){return t!=null?$A(UA(t)):null}function IA(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function GA(t){return t._rawValidators}function WA(t){return t._rawAsyncValidators}function r0(t){return t?Array.isArray(t)?t:[t]:[]}function $p(t,n){return Array.isArray(t)?t.includes(n):t===n}function TA(t,n){let e=r0(n);return r0(t).forEach(r=>{$p(e,r)||e.push(r)}),e}function AA(t,n){return r0(n).filter(e=>!$p(t,e))}var Gp=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=c0(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=d0(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Ki=class extends Gp{name;get formDirective(){return null}get path(){return null}},Xi=class extends Gp{_parent=null;name=null;valueAccessor=null},Wp=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Ln=(()=>{class t extends Wp{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ee(Xi,2))};static \u0275dir=M({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&X("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[ae]})}return t})(),Di=(()=>{class t extends Wp{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ee(Ki,10))};static \u0275dir=M({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&X("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[ae]})}return t})();var Id="VALID",Up="INVALID",gl="PENDING",Td="DISABLED",zo=class{},qp=class extends zo{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Rd=class extends zo{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},kd=class extends zo{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},vl=class extends zo{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Yp=class extends zo{source;constructor(n){super(),this.source=n}},Nd=class extends zo{source;constructor(n){super(),this.source=n}};function u0(t){return(Jp(t)?t.validators:t)||null}function xU(t){return Array.isArray(t)?c0(t):t||null}function f0(t,n){return(Jp(n)?n.asyncValidators:t)||null}function SU(t){return Array.isArray(t)?d0(t):t||null}function Jp(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function qA(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new P(1e3,"");if(!i[e])throw new P(1001,"")}function YA(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new P(-1002,"")})}var _l=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return He(this.statusReactive)}set status(n){He(()=>this.statusReactive.set(n))}_status=Rt(()=>this.statusReactive());statusReactive=S(void 0);get valid(){return this.status===Id}get invalid(){return this.status===Up}get pending(){return this.status===gl}get disabled(){return this.status===Td}get enabled(){return this.status!==Td}errors;get pristine(){return He(this.pristineReactive)}set pristine(n){He(()=>this.pristineReactive.set(n))}_pristine=Rt(()=>this.pristineReactive());pristineReactive=S(!0);get dirty(){return!this.pristine}get touched(){return He(this.touchedReactive)}set touched(n){He(()=>this.touchedReactive.set(n))}_touched=Rt(()=>this.touchedReactive());touchedReactive=S(!1);get untouched(){return!this.touched}_events=new k;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(TA(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(TA(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(AA(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(AA(n,this._rawAsyncValidators))}hasValidator(n){return $p(this._rawValidators,n)}hasAsyncValidator(n){return $p(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(fe(D({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new kd(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new kd(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(fe(D({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Rd(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Rd(!0,i))}markAsPending(n={}){this.status=gl;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new vl(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(fe(D({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Td,this.errors=null,this._forEachChild(r=>{r.disable(fe(D({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new qp(this.value,i)),this._events.next(new vl(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(fe(D({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Id,this._forEachChild(i=>{i.enable(fe(D({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(fe(D({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Id||this.status===gl)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new qp(this.value,e)),this._events.next(new vl(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(fe(D({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Td:Id}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=gl,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=jA(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new vl(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new Z,this.statusChanges=new Z}_calculateStatus(){return this._allControlsDisabled()?Td:this.errors?Up:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(gl)?gl:this._anyControlsHaveStatus(Up)?Up:Id}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new Rd(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new kd(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Jp(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=xU(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=SU(this._rawAsyncValidators)}},yl=class extends _l{constructor(n,e,i){super(u0(e),f0(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){YA(this,!0,n),Object.keys(n).forEach(i=>{qA(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,fe(D({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Nd(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var o0=class extends yl{};var Fd=new C("",{factory:()=>eh}),eh="always";function QA(t,n){return[...n.path,t]}function Qp(t,n,e=eh){m0(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),IU(t,n),AU(t,n),TU(t,n),MU(t,n)}function RA(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Kp(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Zp(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function MU(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function m0(t,n){let e=GA(t);n.validator!==null?t.setValidators(IA(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=WA(t);n.asyncValidator!==null?t.setAsyncValidators(IA(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Zp(n._rawValidators,r),Zp(n._rawAsyncValidators,r)}function Kp(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=GA(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=WA(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Zp(n._rawValidators,i),Zp(n._rawAsyncValidators,i),e}function IU(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&ZA(t,n)})}function TU(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&ZA(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function ZA(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function AU(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function KA(t,n){t==null,m0(t,n)}function RU(t,n){return Kp(t,n)}function XA(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function kU(t){return Object.getPrototypeOf(t.constructor)===fU}function JA(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function eR(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Fn?e=o:kU(o)?i=o:r=o}),r||i||e||null}function OU(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var NU={provide:Ki,useExisting:Rn(()=>Ld)},Ad=Promise.resolve(),Ld=(()=>{class t extends Ki{callSetDisabledState;get submitted(){return He(this.submittedReactive)}_submitted=Rt(()=>this.submittedReactive());submittedReactive=S(!1);_directives=new Set;form;ngSubmit=new Z;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new yl({},c0(e),d0(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Ad.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),Qp(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Ad.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Ad.then(()=>{let i=this._findContainer(e.path),r=new yl({});KA(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Ad.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Ad.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),JA(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Yp(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ee(Ga,10),ee(Xp,10),ee(Fd,8))};static \u0275dir=M({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&F("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[we([NU]),ae]})}return t})();function kA(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function OA(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var Od=class extends _l{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(u0(e),f0(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Jp(e)&&(e.nonNullable||e.initialValueIsDefault)&&(OA(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Nd(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){kA(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){kA(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){OA(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var PU=t=>t instanceof Od;var FU={provide:Xi,useExisting:Rn(()=>p0)},NA=Promise.resolve(),p0=(()=>{class t extends Xi{_changeDetectorRef;callSetDisabledState;control=new Od;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Z;constructor(e,i,r,o,a,s){super(),this._changeDetectorRef=a,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=eR(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),XA(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){Qp(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){NA.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&q(i);NA.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?QA(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(ee(Ki,9),ee(Ga,10),ee(Xp,10),ee(Pd,10),ee(Fe,8),ee(Fd,8))};static \u0275dir=M({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[we([FU]),ae,Pe]})}return t})();var wi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var a0=class extends _l{constructor(n,e,i){super(u0(e),f0(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){YA(this,!1,n),n.forEach((i,r)=>{qA(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],fe(D({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Nd(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var LU=(()=>{class t extends Ki{callSetDisabledState;get submitted(){return He(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Rt(()=>this._submittedReactive());_submittedReactive=S(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Kp(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return Qp(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){RA(e.control||null,e,!1),OU(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,JA(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Yp(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(RA(i||null,e),PU(r)&&(Qp(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);KA(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&RU(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){m0(this.form,this),this._oldForm&&Kp(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ee(Ga,10),ee(Xp,10),ee(Fd,8))};static \u0275dir=M({type:t,features:[ae,Pe]})}return t})();var tR=new C("");var VU={provide:Xi,useExisting:Rn(()=>ni)},ni=(()=>{class t extends Xi{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new Z;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=eR(this,o)}ngOnChanges(e){this._added||this._setUpControl(),XA(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return QA(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(ee(Ki,13),ee(Ga,10),ee(Xp,10),ee(Pd,10),ee(tR,8))};static \u0275dir=M({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[we([VU]),ae,Pe]})}return t})();var jU={provide:Ki,useExisting:Rn(()=>Yt)},Yt=(()=>{class t extends LU{form=null;ngSubmit=new Z;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&F("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[we([jU]),ae]})}return t})();var BU=(()=>{class t{_validator=zp;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):zp,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,features:[Pe]})}return t})();var HU={provide:Ga,useExisting:Rn(()=>ii),multi:!0};var ii=(()=>{class t extends BU{required;inputName="required";normalizeInput=q;createValidator=e=>LA;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&de("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[we([HU]),ae]})}return t})();var nR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})();function PA(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var UU=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return PA(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new yl(r,o)}record(e,i=null){let r=this._reduceControls(e);return new o0(r,i)}control(e,i,r){let o={};return this.useNonNullable?(PA(i)?o=i:(o.validators=i,o.asyncValidators=r),new Od(e,fe(D({},o),{nonNullable:!0}))):new Od(e,i,r)}array(e,i,r){let o=e.map(a=>this._createControl(a));return new a0(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof Od)return e;if(e instanceof _l)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var th=(()=>{class t extends UU{group(e,i=null){return super.group(e,i)}control(e,i,r){return super.control(e,i,r)}array(e,i,r){return super.array(e,i,r)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),iR=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Fd,useValue:e.callSetDisabledState??eh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[nR]})}return t})(),h0=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:tR,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:Fd,useValue:e.callSetDisabledState??eh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[nR]})}return t})();var rR=(()=>{class t{_animationsDisabled=tt();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&X("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2,changeDetection:0})}return t})();var zU=["text"],$U=[[["mat-icon"]],"*"],GU=["mat-icon","*"];function WU(t,n){if(t&1&&z(0,"mat-pseudo-checkbox",1),t&2){let e=b();E("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function qU(t,n){if(t&1&&z(0,"mat-pseudo-checkbox",3),t&2){let e=b();E("disabled",e.disabled)}}function YU(t,n){if(t&1&&(f(0,"span",4),g(1),p()),t&2){let e=b();h(),$("(",e.group.label,")")}}var ih=new C("MAT_OPTION_PARENT_COMPONENT"),rh=new C("MatOptgroup");var nh=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},_n=(()=>{class t{_element=d(W);_changeDetectorRef=d(Fe);_parent=d(ih,{optional:!0});group=d(rh,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ue).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=S(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new Z;_text;_stateChanges=new k;constructor(){let e=d(_t);e.load(Ci),e.load(fl),this._signalDisableRipple=!!this._parent&&br(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ot(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new nh(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&dt(zU,7),i&2){let o;Y(o=Q())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&F("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(xt("id",r.id),de("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),X("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",q]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:GU,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Me($U),T(0,WU,1,2,"mat-pseudo-checkbox",1),K(1),f(2,"span",2,0),K(4,1),p(),T(5,qU,1,1,"mat-pseudo-checkbox",3),T(6,YU,2,1,"span",4),z(7,"div",5)),i&2&&(A(r.multiple?0:-1),h(5),A(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),h(),A(r.group&&r.group._inert?6:-1),h(),E("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[rR,jo],styles:[`.mat-mdc-option {
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
`],encapsulation:2,changeDetection:0})}return t})();function g0(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function v0(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var _0=class{_box;_destroyed=new k;_resizeSubject=new k;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ye(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ie(e=>e.some(i=>i.target===n)),Pu({bufferSize:1,refCount:!0}),Te(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},oR=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(re);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new _0(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var QU=["notch"],ZU=["matFormFieldNotchedOutline",""],KU=["*"],aR=["iconPrefixContainer"],sR=["textPrefixContainer"],lR=["iconSuffixContainer"],cR=["textSuffixContainer"],XU=["textField"],JU=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],ez=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function tz(t,n){t&1&&z(0,"span",21)}function nz(t,n){if(t&1&&(f(0,"label",20),K(1,1),T(2,tz,1,0,"span",21),p()),t&2){let e=b(2);E("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),de("for",e._control.disableAutomaticLabeling?null:e._control.id),h(2),A(!e.hideRequiredMarker&&e._control.required?2:-1)}}function iz(t,n){if(t&1&&T(0,nz,3,5,"label",20),t&2){let e=b();A(e._hasFloatingLabel()?0:-1)}}function rz(t,n){t&1&&z(0,"div",7)}function oz(t,n){}function az(t,n){if(t&1&&N(0,oz,0,0,"ng-template",13),t&2){b(2);let e=zt(1);E("ngTemplateOutlet",e)}}function sz(t,n){if(t&1&&(f(0,"div",9),T(1,az,1,1,null,13),p()),t&2){let e=b();E("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),h(),A(e._forceDisplayInfixLabel()?-1:1)}}function lz(t,n){t&1&&(f(0,"div",10,2),K(2,2),p())}function cz(t,n){t&1&&(f(0,"div",11,3),K(2,3),p())}function dz(t,n){}function uz(t,n){if(t&1&&N(0,dz,0,0,"ng-template",13),t&2){b();let e=zt(1);E("ngTemplateOutlet",e)}}function fz(t,n){t&1&&(f(0,"div",14,4),K(2,4),p())}function mz(t,n){t&1&&(f(0,"div",15,5),K(2,5),p())}function pz(t,n){t&1&&z(0,"div",16)}function hz(t,n){t&1&&(f(0,"div",18),K(1,6),p())}function gz(t,n){if(t&1&&(f(0,"mat-hint",22),g(1),p()),t&2){let e=b(2);E("id",e._hintLabelId),h(),me(e.hintLabel)}}function vz(t,n){if(t&1&&(f(0,"div",19),T(1,gz,2,2,"mat-hint",22),K(2,7),z(3,"div",23),K(4,8),p()),t&2){let e=b();h(),A(e.hintLabel?1:-1)}}var rn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-label"]]})}return t})(),gR=new C("MatError"),jn=(()=>{class t{id=d(Ue).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&xt("id",r.id)},inputs:{id:"id"},features:[we([{provide:gR,useExisting:t}])]})}return t})(),Vn=(()=>{class t{align="start";id=d(Ue).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(xt("id",r.id),de("align",null),X("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),_z=new C("MatPrefix");var yz=new C("MatSuffix");var vR=new C("FloatingLabelParent"),dR=(()=>{class t{_elementRef=d(W);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(oR);_ngZone=d(re);_parent=d(vR);_resizeSubscription=new pe;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return bz(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&X("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function bz(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var uR="mdc-line-ripple--active",oh="mdc-line-ripple--deactivating",fR=(()=>{class t{_elementRef=d(W);_cleanupTransitionEnd;constructor(){let e=d(re),i=d(qe);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(oh),e.add(uR)}deactivate(){this._elementRef.nativeElement.classList.add(oh)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(oh);e.propertyName==="opacity"&&r&&i.remove(uR,oh)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),mR=(()=>{class t{_elementRef=d(W);_ngZone=d(re);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&dt(QU,5),i&2){let o;Y(o=Q())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&X("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:ZU,ngContentSelectors:KU,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Me(),Jt(0,"div",1),Et(1,"div",2,0),K(3),Lt(),Jt(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),Vd=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t})}return t})();var jd=new C("MatFormField"),Cz=new C("MAT_FORM_FIELD_DEFAULT_OPTIONS"),pR="fill",Dz="auto",hR="fixed",wz="translateY(-50%)",yn=(()=>{class t{_elementRef=d(W);_changeDetectorRef=d(Fe);_platform=d(ke);_idGenerator=d(Ue);_ngZone=d(re);_defaults=d(Cz,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Rc("iconPrefixContainer");_textPrefixContainerSignal=Rc("textPrefixContainer");_iconSuffixContainerSignal=Rc("iconSuffixContainer");_textSuffixContainerSignal=Rc("textSuffixContainer");_prefixSuffixContainers=Rt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=DM(rn);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Kr(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||Dz}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||pR;this._appearanceSignal.set(i)}_appearanceSignal=S(pR);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||hR}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||hR}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new k;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=tt();constructor(){let e=this._defaults,i=d(Mt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Li(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Rt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(We([void 0,void 0]),he(()=>[i.errorState,i.userAriaDescribedBy]),Nu(),Ie(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Te(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),an(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){MM({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Rt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",m=`${a+s}px`,_=`calc(${u} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,y=`var(--mat-mdc-form-field-label-transform, ${wz} translateX(${_}))`,I=a+s+l+c;return[y,I]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(hm(o,r._labelChild,rn,5),rt(o,Vd,5)(o,_z,5)(o,yz,5)(o,gR,5)(o,Vn,5)),i&2){vm();let a;Y(a=Q())&&(r._formFieldControl=a.first),Y(a=Q())&&(r._prefixChildren=a),Y(a=Q())&&(r._suffixChildren=a),Y(a=Q())&&(r._errorChildren=a),Y(a=Q())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(gm(r._iconPrefixContainerSignal,aR,5)(r._textPrefixContainerSignal,sR,5)(r._iconSuffixContainerSignal,lR,5)(r._textSuffixContainerSignal,cR,5),dt(XU,5)(aR,5)(sR,5)(lR,5)(cR,5)(dR,5)(mR,5)(fR,5)),i&2){vm(4);let o;Y(o=Q())&&(r._textField=o.first),Y(o=Q())&&(r._iconPrefixContainer=o.first),Y(o=Q())&&(r._textPrefixContainer=o.first),Y(o=Q())&&(r._iconSuffixContainer=o.first),Y(o=Q())&&(r._textSuffixContainer=o.first),Y(o=Q())&&(r._floatingLabel=o.first),Y(o=Q())&&(r._notchedOutline=o.first),Y(o=Q())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&X("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[we([{provide:jd,useExisting:t},{provide:vR,useExisting:t}])],ngContentSelectors:ez,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Me(JU),N(0,iz,1,1,"ng-template",null,0,Ls),f(2,"div",6,1),F("click",function(a){return r._control.onContainerClick(a)}),T(4,rz,1,0,"div",7),f(5,"div",8),T(6,sz,2,2,"div",9),T(7,lz,3,0,"div",10),T(8,cz,3,0,"div",11),f(9,"div",12),T(10,uz,1,1,null,13),K(11),p(),T(12,fz,3,0,"div",14),T(13,mz,3,0,"div",15),p(),T(14,pz,1,0,"div",16),p(),f(15,"div",17),T(16,hz,2,0,"div",18)(17,vz,5,1,"div",19),p()),i&2){let o;h(2),X("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),h(2),A(!r._hasOutline()&&!r._control.disabled?4:-1),h(2),A(r._hasOutline()?6:-1),h(),A(r._hasIconPrefix?7:-1),h(),A(r._hasTextPrefix?8:-1),h(2),A(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),h(2),A(r._hasTextSuffix?12:-1),h(),A(r._hasIconSuffix?13:-1),h(),A(r._hasOutline()?-1:14),h(),X("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();h(),A((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[dR,mR,Bs,fR,Vn],styles:[`.mdc-text-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var _R=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var y0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Bo,_R,_n,Ce]})}return t})();var Bd=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new k;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Hd=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var sh=class{applyChanges(n,e,i,r,o){n.forEachOperation((a,s,l)=>{let c,u;if(a.previousIndex==null){let m=i(a,s,l);c=e.createEmbeddedView(m.templateRef,m.context,m.index),u=Yi.INSERTED}else l==null?(e.remove(s),u=Yi.REMOVED):(c=e.get(s),e.move(c,l),u=Yi.MOVED);o&&o({context:c?.context,operation:u,record:a})})}detach(){}};var Ez=["*"];var xz=new C("MAT_CARD_CONFIG"),Ji=(()=>{class t{appearance;constructor(){let e=d(xz,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&X("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:Ez,decls:1,vars:0,template:function(i,r){i&1&&(Me(),K(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2,changeDetection:0})}return t})(),er=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var tr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var b0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var Sz=["mat-internal-form-field",""],Mz=["*"],yR=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&X("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:Sz,ngContentSelectors:Mz,decls:1,vars:0,template:function(i,r){i&1&&(Me(),K(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var $o=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Cl=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var C0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var bR=new C("MAT_INPUT_VALUE_ACCESSOR");var D0=new C("CdkAccordion"),CR=(()=>{class t{_stateChanges=new k;_openCloseAllActions=new k;id=d(Ue).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",q]},exportAs:["cdkAccordion"],features:[we([{provide:D0,useExisting:t}]),Pe]})}return t})(),DR=(()=>{class t{accordion=d(D0,{optional:!0,skipSelf:!0});_changeDetectorRef=d(Fe);_expansionDispatcher=d(Hd);_openCloseAllSubscription=pe.EMPTY;closed=new Z;opened=new Z;destroyed=new Z;expandedChange=new Z;id=d(Ue).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=S(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",q],disabled:[2,"disabled","disabled",q]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[we([{provide:D0,useValue:void 0}])]})}return t})(),wR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})();var Iz=["body"],Tz=["bodyWrapper"],Az=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],Rz=["mat-expansion-panel-header","*","mat-action-row"];function kz(t,n){}var Oz=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],Nz=["mat-panel-title","mat-panel-description","*"];function Pz(t,n){t&1&&(Et(0,"span",1),hr(),Et(1,"svg",2),Jt(2,"path",3),Lt()())}var w0=new C("MAT_ACCORDION"),ER=new C("MAT_EXPANSION_PANEL"),Fz=(()=>{class t{_template=d(Ve);_expansionPanel=d(ER,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),xR=new C("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),ri=(()=>{class t extends DR{_viewContainerRef=d(ct);_animationsDisabled=tt();_document=d(j);_ngZone=d(re);_elementRef=d(W);_renderer=d(qe);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new Z;afterCollapse=new Z;_inputChanges=new k;accordion=d(w0,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=d(Ue).getId("mat-expansion-panel-header-");constructor(){super();let e=d(xR,{optional:!0});this._expansionDispatcher=d(Hd),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(We(null),Ie(()=>this.expanded&&!this._portal),lt(1)).subscribe(()=>{this._portal=new vn(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&rt(o,Fz,5),i&2){let a;Y(a=Q())&&(r._lazyContent=a.first)}},viewQuery:function(i,r){if(i&1&&dt(Iz,5)(Tz,5),i&2){let o;Y(o=Q())&&(r._body=o.first),Y(o=Q())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&X("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",q],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[we([{provide:w0,useValue:void 0},{provide:ER,useExisting:t}]),ae,Pe],ngContentSelectors:Rz,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Me(Az),K(0),f(1,"div",2,0)(3,"div",3,1)(5,"div",4),K(6,1),N(7,kz,0,0,"ng-template",5),p(),K(8,2),p()()),i&2&&(h(),de("inert",r.expanded?null:""),h(2),E("id",r.id),de("aria-labelledby",r._headerId),h(4),E("cdkPortalOutlet",r._portal))},dependencies:[vi],styles:[`.mat-expansion-panel {
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
`],encapsulation:2,changeDetection:0})}return t})(),SR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-action-row"]],hostAttrs:[1,"mat-action-row"]})}return t})(),oi=(()=>{class t{panel=d(ri,{host:!0});_element=d(W);_focusMonitor=d(Pn);_changeDetectorRef=d(Fe);_parentChangeSubscription=pe.EMPTY;constructor(){d(_t).load(Ci);let e=this.panel,i=d(xR,{optional:!0}),r=d(new kn("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(Ie(a=>!!(a.hideToggle||a.togglePosition))):ft;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=an(e.opened,e.closed,o,e._inputChanges.pipe(Ie(a=>!!(a.hideToggle||a.disabled||a.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(Ie(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:Ot(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&F("click",function(){return r._toggle()})("keydown",function(a){return r._keydown(a)}),i&2&&(de("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),x("height",r._getHeaderHeight()),X("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:zi(e)]},ngContentSelectors:Nz,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(Me(Oz),Et(0,"span",0),K(1),K(2,1),K(3,2),Lt(),T(4,Pz,3,0,"span",1)),i&2&&(X("mat-content-hide-toggle",!r._showToggle()),h(4),A(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
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
`],encapsulation:2,changeDetection:0})}return t})(),Ir=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-panel-description"]],hostAttrs:[1,"mat-expansion-panel-header-description"]})}return t})(),Ei=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),Tr=(()=>{class t extends CR{_keyManager;_ownHeaders=new Xt;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(We(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new Xr(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&rt(o,oi,5),i&2){let a;Y(a=Q())&&(r._headers=a)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&X("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",q],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[we([{provide:w0,useExisting:t}]),ae]})}return t})(),lh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[wR,_i,Ce]})}return t})();var Go=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Fp,yn,Ce]})}return t})();var ch=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,e){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=e.map(i=>this._trackTile(i))}_trackTile(n){let e=this._findMatchingGap(n.colspan);return this._markTilePosition(e,n),this.columnIndex=e+n.colspan,new E0(this.rowIndex,e)}_findMatchingGap(n){n>this.tracker.length;let e=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}if(e=this.tracker.indexOf(0,this.columnIndex),e==-1){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}i=this._findGapEndIndex(e),this.columnIndex=e+1}while(i-e<n||i==0);return Math.max(e,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let e=n+1;e<this.tracker.length;e++)if(this.tracker[e]!=0)return e;return this.tracker.length}_markTilePosition(n,e){for(let i=0;i<e.colspan;i++)this.tracker[n+i]=e.rowspan}},E0=class{row;col;constructor(n,e){this.row=n,this.col=e}};var MR=["*"];var jz=`.mat-grid-list {
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
`,IR=new C("MAT_GRID_LIST"),Bz=(()=>{class t{_element=d(W);_gridList=d(IR,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(e){this._rowspan=Math.round(xr(e))}get colspan(){return this._colspan}set colspan(e){this._colspan=Math.round(xr(e))}_setStyle(e,i){this._element.nativeElement.style[e]=i}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&de("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:MR,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(Me(),Et(0,"div",0),K(1),Lt())},styles:[`.mat-grid-list {
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
`],encapsulation:2,changeDetection:0})}return t})();var Hz=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,zd=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,e,i,r){this._gutterSize=TR(n),this._rows=e.rowCount,this._rowspan=e.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,e){return`(${n}% - (${this._gutterSize} * ${e}))`}getTilePosition(n,e){return e===0?"0":Wa(`(${n} + ${this._gutterSize}) * ${e}`)}getTileSize(n,e){return`(${n} * ${e}) + (${e-1} * ${this._gutterSize})`}setStyle(n,e,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,e,r,o)}setColStyles(n,e,i,r){let o=this.getBaseTileSize(i,r),a=this._direction==="rtl"?"right":"left";n._setStyle(a,this.getTilePosition(o,e)),n._setStyle("width",Wa(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},x0=class extends zd{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,e,i,r){super.init(n,e,i,r),this.fixedRowHeight=TR(this.fixedRowHeight),Hz.test(this.fixedRowHeight)}setRowStyles(n,e){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,e)),n._setStyle("height",Wa(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",Wa(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}},S0=class extends zd{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,e,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,e)),n._setStyle("paddingTop",Wa(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",Wa(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(e=>{e._setStyle("marginTop",null),e._setStyle("paddingTop",null)})}_parseRatio(n){let e=n.split(":");e.length,this.rowHeightRatio=parseFloat(e[0])/parseFloat(e[1])}},M0=class extends zd{setRowStyles(n,e){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,e)),n._setStyle("height",Wa(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}};function Wa(t){return`calc(${t})`}function TR(t){return t.match(/([A-Za-z%]+)$/)?t:`${t}px`}var Uz="fit",nr=(()=>{class t{_element=d(W);_dir=d(Mt,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(e){this._cols=Math.max(1,Math.round(xr(e)))}get gutterSize(){return this._gutter}set gutterSize(e){this._gutter=`${e??""}`}get rowHeight(){return this._rowHeight}set rowHeight(e){let i=`${e??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(e){this._tileStyler&&this._tileStyler.reset(this),e===Uz?this._tileStyler=new M0:e&&e.indexOf(":")>-1?this._tileStyler=new S0(e):this._tileStyler=new x0(e)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new ch);let e=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,e,this.cols,r),i.forEach((o,a)=>{let s=e.positions[a];this._tileStyler.setStyle(o,s.row,s.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(e){e&&(this._element.nativeElement.style[e[0]]=e[1])}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&rt(o,Bz,5),i&2){let a;Y(a=Q())&&(r._tiles=a)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&de("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[we([{provide:IR,useExisting:t}])],ngContentSelectors:MR,decls:2,vars:0,template:function(i,r){i&1&&(Me(),Et(0,"div"),K(1),Lt())},styles:[jz],encapsulation:2,changeDetection:0})}return t})(),I0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[C0,Ce,C0]})}return t})();function AR(t){return Error(`Unable to find icon with the name "${t}"`)}function zz(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function RR(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function kR(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var io=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},NR=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new io(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(Ft.HTML,r);if(!a)throw kR(r);let s=Ua(a);return this._addSvgIconConfig(e,i,new io("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new io(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Ft.HTML,i);if(!o)throw kR(i);let a=Ua(o);return this._addSvgIconSetConfig(e,new io("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Ft.RESOURCE_URL,e);if(!i)throw RR(e);let r=this._cachedIconsByUrl.get(i);return r?J(dh(r)):this._loadSvgIconFromConfig(new io(e,null)).pipe(jt(o=>this._cachedIconsByUrl.set(i,o)),he(o=>dh(o)))}getNamedSvgIcon(e,i=""){let r=OR(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):jl(AR(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?J(dh(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(he(i=>dh(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return J(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(ho(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(Ft.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),J(null)})));return Bl(o).pipe(he(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw AR(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(jt(i=>e.svgText=i),he(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?J(null):this._fetchIcon(e).pipe(jt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(Ua("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(Ua("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw zz();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(Ft.RESOURCE_URL,i);if(!a)throw RR(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(he(c=>Ua(c)),aa(()=>this._inProgressUrlFetches.delete(a)),Ul());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(OR(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return $z(o)?new io(o.url,null,o.options):new io(o,null)}}static \u0275fac=function(i){return new(i||t)(se(qy,8),se(Uc),se(j,8),se(ln))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function dh(t){return t.cloneNode(!0)}function OR(t,n){return t+":"+n}function $z(t){return!!(t.url&&t.options)}var Gz=["*"],Wz=new C("MAT_ICON_DEFAULT_OPTIONS"),qz=new C("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(j),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),PR=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],Yz=PR.map(t=>`[${t}]`).join(", "),Qz=/^url\(['"]?#(.*?)['"]?\)$/,Bn=(()=>{class t{_elementRef=d(W);_iconRegistry=d(NR);_location=d(qz);_errorHandler=d(ln);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=pe.EMPTY;constructor(){let e=d(new kn("aria-hidden"),{optional:!0}),i=d(Wz,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(Yz),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)PR.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(Qz):null;if(c){let u=r.get(s);u||(u=[],r.set(s,u)),u.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(lt(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(de("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),$t(r.color?"mat-"+r.color:""),X("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",q],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:Gz,decls:1,vars:0,template:function(i,r){i&1&&(Me(),K(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2,changeDetection:0})}return t})(),Wo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var Zz=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2,changeDetection:0})}return t})(),Kz={passive:!0},FR=(()=>{class t{_platform=d(ke);_ngZone=d(re);_renderer=d(Tt).createRenderer(null,null);_styleLoader=d(_t);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return ft;this._styleLoader.load(Zz);let i=ti(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new k,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,Kz)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=ti(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var LR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({})}return t})();var Xz=["button","checkbox","file","hidden","image","radio","range","reset","submit"],Jz=new C("MAT_INPUT_CONFIG"),xi=(()=>{class t{_elementRef=d(W);_platform=d(ke);ngControl=d(Xi,{optional:!0,self:!0});_autofillMonitor=d(FR);_ngZone=d(re);_formField=d(jd,{optional:!0});_renderer=d(qe);_uid=d(Ue).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(Jz,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new k;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Kr(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(H.required)??!1}set required(e){this._required=Kr(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Qb().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Kr(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Qb().has(e));constructor(){let e=d(Ld,{optional:!0}),i=d(Yt,{optional:!0}),r=d($o),o=d(bR,{optional:!0,self:!0}),a=this._elementRef.nativeElement,s=a.nodeName.toLowerCase();o?br(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Cl(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=s==="select",this._isTextarea=s==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Li(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){Xz.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&F("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(xt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),de("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),X("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",q]},exportAs:["matInput"],features:[we([{provide:Vd,useExisting:t}]),Pe]})}return t})(),T0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Go,Go,LR,Ce]})}return t})();var e$=["mat-menu-item",""],t$=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],n$=["mat-icon, [matMenuItemIcon]","*"];function i$(t,n){t&1&&(hr(),f(0,"svg",2),z(1,"polygon",3),p())}var r$=["*"];function o$(t,n){if(t&1){let e=Ke();Et(0,"div",0),pm("click",function(){Ae(e);let r=b();return Re(r.closed.emit("click"))})("animationstart",function(r){Ae(e);let o=b();return Re(o._onAnimationStart(r.animationName))})("animationend",function(r){Ae(e);let o=b();return Re(o._onAnimationDone(r.animationName))})("animationcancel",function(r){Ae(e);let o=b();return Re(o._onAnimationDone(r.animationName))}),Et(1,"div",1),K(2),Lt()()}if(t&2){let e=b();$t(e._classList),X("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),xt("id",e.panelId),de("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var R0=new C("MAT_MENU_PANEL"),Wd=(()=>{class t{_elementRef=d(W);_document=d(j);_focusMonitor=d(Pn);_parentMenu=d(R0,{optional:!0});_changeDetectorRef=d(Fe);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new k;_focused=new k;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(_t).load(Ci),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&F("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(de("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),X("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",q],disableRipple:[2,"disableRipple","disableRipple",q]},exportAs:["matMenuItem"],attrs:e$,ngContentSelectors:n$,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Me(t$),K(0),f(1,"span",0),K(2,1),p(),z(3,"div",1),T(4,i$,2,0,":svg:svg",2)),i&2&&(h(3),E("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),h(),A(r._triggersSubmenu?4:-1))},dependencies:[jo],encapsulation:2,changeDetection:0})}return t})();var a$=new C("MatMenuContent");var s$=new C("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),A0="_mat-menu-enter",uh="_mat-menu-exit",xl=(()=>{class t{_elementRef=d(W);_changeDetectorRef=d(Fe);_injector=d(oe);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=tt();_allItems;_directDescendantItems=new Xt;_classList={};_panelAnimationState="void";_animationDone=new k;_isAnimating=S(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=D({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new Z;close=this.closed;panelId=d(Ue).getId("mat-menu-panel-");constructor(){let e=d(s$);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Xr(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(We(this._directDescendantItems),mt(e=>an(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(We(this._directDescendantItems),mt(i=>an(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Ot(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=ht(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=fe(D({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===uh;(i||e===A0)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===A0||e===uh)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(uh),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?A0:uh)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(We(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&rt(o,a$,5)(o,Wd,5)(o,Wd,4),i&2){let a;Y(a=Q())&&(r.lazyContent=a.first),Y(a=Q())&&(r._allItems=a),Y(a=Q())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&dt(Ve,5),i&2){let o;Y(o=Q())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&de("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",q],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:q(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[we([{provide:R0,useExisting:t}])],ngContentSelectors:r$,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Me(),Os(0,o$,3,12,"ng-template"))},styles:[`mat-menu {
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
`],encapsulation:2,changeDetection:0})}return t})(),l$=new C("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>ja(t)}});var El=new WeakMap,c$=(()=>{class t{_canHaveBackdrop;_element=d(W);_viewContainerRef=d(ct);_menuItemInstance=d(Wd,{optional:!0,self:!0});_dir=d(Mt,{optional:!0});_focusMonitor=d(Pn);_ngZone=d(re);_injector=d(oe);_scrollStrategy=d(l$);_changeDetectorRef=d(Fe);_animationsDisabled=tt();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=pe.EMPTY;_menuCloseSubscription=pe.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(R0,{optional:!0});this._parentMaterialMenu=i instanceof xl?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&El.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=El.get(i);El.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof xl&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(Te(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof xl&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(lt(1)).subscribe(()=>{i.detach(),El.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&El.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Zr(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof xl&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Qi({positionStrategy:fd(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[a,s],[u,m]=[r,o],v=0;if(this._triggersSubmenu()){if(m=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}v=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=a==="top"?"bottom":"top",c=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:v},{originX:o,originY:l,overlayX:m,overlayY:a,offsetY:v},{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:-v},{originX:o,originY:c,overlayX:m,overlayY:s,offsetY:-v}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:J(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(Ie(a=>this._menuOpen&&a!==this._menuItemInstance)):J();return an(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new vn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return El.get(e)===this}_triggerIsAriaDisabled(){return q(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){cm()};static \u0275dir=M({type:t})}return t})(),VR=(()=>{class t extends c${_cleanupTouchstart;_hoverSubscription=pe.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new Z;onMenuOpen=this.menuOpened;menuClosed=new Z;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(qe);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{Ha(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){Ba(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&F("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&de("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[ae]})}return t})();var jR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Bo,Mr,Ce,sl]})}return t})();var u$=["trigger"],f$=["panel"],m$=[[["mat-select-trigger"]],"*"],p$=["mat-select-trigger","*"];function h$(t,n){if(t&1&&(f(0,"span",4),g(1),p()),t&2){let e=b();h(),me(e.placeholder)}}function g$(t,n){t&1&&K(0)}function v$(t,n){if(t&1&&(f(0,"span",11),g(1),p()),t&2){let e=b(2);h(),me(e.triggerValue)}}function _$(t,n){if(t&1&&(f(0,"span",5),T(1,g$,1,0)(2,v$,2,1,"span",11),p()),t&2){let e=b();h(),A(e.customTrigger?1:2)}}function y$(t,n){if(t&1){let e=Ke();f(0,"div",12,1),F("keydown",function(r){Ae(e);let o=b();return Re(o._handleKeydown(r))}),K(2,1),p()}if(t&2){let e=b();$t(e.panelClass),X("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),de("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var b$=new C("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(oe);return()=>ja(t)}}),C$=new C("MAT_SELECT_CONFIG"),D$=new C("MatSelectTrigger"),k0=class{source;value;constructor(n,e){this.source=n,this.value=e}},Ar=(()=>{class t{_viewportRuler=d(Sr);_changeDetectorRef=d(Fe);_elementRef=d(W);_dir=d(Mt,{optional:!0});_idGenerator=d(Ue);_renderer=d(qe);_parentFormField=d(jd,{optional:!0});ngControl=d(Xi,{self:!0,optional:!0});_liveAnnouncer=d(vd);_defaultOptions=d(C$,{optional:!0});_animationsDisabled=tt();_popoverLocation;_initialized=new k;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=g0(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=v0(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new k0(this,e)}_scrollStrategyFactory=d(b$);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new k;_errorStateTracker;stateChanges=new k;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=S(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(H.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Ai(()=>{let e=this.options;return e?e.changes.pipe(We(e),mt(()=>an(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(mt(()=>this.optionSelectionChanges))});openedChange=new Z;_openedStream=this.openedChange.pipe(Ie(e=>e),he(()=>{}));_closedStream=this.openedChange.pipe(Ie(e=>!e),he(()=>{}));selectionChange=new Z;valueChange=new Z;constructor(){let e=d($o),i=d(Ld,{optional:!0}),r=d(Yt,{optional:!0}),o=d(new kn("tabindex"),{optional:!0}),a=d(md,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Cl(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Bd(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Te(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Te(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(We(null),Te(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(lt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&Gb(this._trackedModal,"aria-owns",i),cA(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Gb(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!Ot(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!Ot(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ot(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof cl?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new wd(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=an(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Te(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),an(...this.options.map(i=>i._stateChanges)).pipe(Te(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Wt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&rt(o,D$,5)(o,_n,5)(o,rh,5),i&2){let a;Y(a=Q())&&(r.customTrigger=a.first),Y(a=Q())&&(r.options=a),Y(a=Q())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&dt(u$,5)(f$,5)(kp,5),i&2){let o;Y(o=Q())&&(r.trigger=o.first),Y(o=Q())&&(r.panel=o.first),Y(o=Q())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&F("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(de("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),X("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",q],disableRipple:[2,"disableRipple","disableRipple",q],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:zi(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",q],placeholder:"placeholder",required:[2,"required","required",q],multiple:[2,"multiple","multiple",q],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",q],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",zi],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",q]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[we([{provide:Vd,useExisting:t},{provide:ih,useExisting:t}]),Pe],ngContentSelectors:p$,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Me(m$),f(0,"div",2,0),F("click",function(){return r.open()}),f(3,"div",3),T(4,h$,2,1,"span",4)(5,_$,3,1,"span",5),p(),f(6,"div",6)(7,"div",7),hr(),f(8,"svg",8),z(9,"path",9),p()()()(),N(10,y$,3,16,"ng-template",10),F("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=zt(1);h(3),de("id",r._valueId),h(),A(r.empty?4:5),h(6),E("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[cl,kp],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2,changeDetection:0})}return t})();var O0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Mr,y0,Ce,sl,Go,y0]})}return t})();var w$=["switch"],E$=["*"];function x$(t,n){t&1&&(f(0,"span",11),hr(),f(1,"svg",13),z(2,"path",14),p(),f(3,"svg",15),z(4,"path",16),p()())}var S$=new C("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),mh=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},N0=(()=>{class t{_elementRef=d(W);_focusMonitor=d(Pn);_changeDetectorRef=d(Fe);defaults=d(S$);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new mh(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=tt();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new Z;toggleChange=new Z;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(_t).load(Ci);let e=d(new kn("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(Ue).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new mh(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&dt(w$,5),i&2){let o;Y(o=Q())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(xt("id",r.id),de("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),$t(r.color?"mat-"+r.color:""),X("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",q],color:"color",disabled:[2,"disabled","disabled",q],disableRipple:[2,"disableRipple","disableRipple",q],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:zi(e)],checked:[2,"checked","checked",q],hideIcon:[2,"hideIcon","hideIcon",q],disabledInteractive:[2,"disabledInteractive","disabledInteractive",q]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[we([{provide:Pd,useExisting:Rn(()=>t),multi:!0},{provide:Ga,useExisting:t,multi:!0}]),Pe],ngContentSelectors:E$,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(Me(),f(0,"div",1)(1,"button",2,0),F("click",function(){return r._handleClick()}),z(3,"div",3)(4,"span",4),f(5,"span",5)(6,"span",6)(7,"span",7),z(8,"span",8),p(),f(9,"span",9),z(10,"span",10),p(),T(11,x$,5,0,"span",11),p()()(),f(12,"label",12),F("click",function(a){return a.stopPropagation()}),K(13),p()()),i&2){let o=zt(2);E("labelPosition",r.labelPosition),h(),X("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),E("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),de("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),h(9),E("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),h(),A(r.hideIcon?-1:11),h(),E("for",r.buttonId),de("id",r._labelId)}},dependencies:[jo,yR],styles:[`.mdc-switch {
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
`],encapsulation:2,changeDetection:0})}return t})(),BR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[N0,Ce]})}return t})();var I$=[[["caption"]],[["colgroup"],["col"]],"*"],T$=["caption","colgroup, col","*"];function A$(t,n){t&1&&K(0,2)}function R$(t,n){t&1&&(f(0,"thead",0),it(1,1),p(),f(2,"tbody",0),it(3,2)(4,3),p(),f(5,"tfoot",0),it(6,4),p())}function k$(t,n){t&1&&it(0,1)(1,2)(2,3)(3,4)}var ir=new C("CDK_TABLE");var gh=(()=>{class t{template=d(Ve);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkCellDef",""]]})}return t})(),vh=(()=>{class t{template=d(Ve);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkHeaderCellDef",""]]})}return t})(),zR=(()=>{class t{template=d(Ve);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkFooterCellDef",""]]})}return t})(),Sl=(()=>{class t{_table=d(ir,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(e){this._setNameInput(e)}_name;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(e){e!==this._stickyEnd&&(this._stickyEnd=e,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(e){e&&(this._name=e,this.cssClassFriendlyName=e.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkColumnDef",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,gh,5)(o,vh,5)(o,zR,5),i&2){let a;Y(a=Q())&&(r.cell=a.first),Y(a=Q())&&(r.headerCell=a.first),Y(a=Q())&&(r.footerCell=a.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",q],stickyEnd:[2,"stickyEnd","stickyEnd",q]}})}return t})(),hh=class{constructor(n,e){e.nativeElement.classList.add(...n._columnCssClassName)}},$R=(()=>{class t extends hh{constructor(){super(d(Sl),d(W))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[ae]})}return t})();var GR=(()=>{class t extends hh{constructor(){let e=d(Sl),i=d(W);super(e,i);let r=e._table?._getCellRole();r&&i.nativeElement.setAttribute("role",r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[ae]})}return t})();var F0=(()=>{class t{template=d(Ve);_differs=d(Ur);columns;_columnsDiffer;constructor(){}ngOnChanges(e){if(!this._columnsDiffer){let i=e.columns&&e.columns.currentValue||[];this._columnsDiffer=this._differs.find(i).create(),this._columnsDiffer.diff(i)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(e){return this instanceof Yd?e.headerCell.template:this instanceof L0?e.footerCell.template:e.cell.template}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,features:[Pe]})}return t})(),Yd=(()=>{class t extends F0{_table=d(ir,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(Ve),d(Ur))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",q]},features:[ae,Pe]})}return t})(),L0=(()=>{class t extends F0{_table=d(ir,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(Ve),d(Ur))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",q]},features:[ae,Pe]})}return t})(),_h=(()=>{class t extends F0{_table=d(ir,{optional:!0});when;constructor(){super(d(Ve),d(Ur))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[ae]})}return t})(),qa=(()=>{class t{_viewContainer=d(ct);cells;context;static mostRecentCellOutlet=null;constructor(){t.mostRecentCellOutlet=this}ngOnDestroy(){t.mostRecentCellOutlet===this&&(t.mostRecentCellOutlet=null)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkCellOutlet",""]]})}return t})(),V0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[qa],encapsulation:2})}return t})();var j0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[qa],encapsulation:2})}return t})(),WR=(()=>{class t{templateRef=d(Ve);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","cdkNoDataRow",""]]})}return t})(),HR=["top","bottom","left","right"],P0=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(n=>this._updateCachedSizes(n)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(n,e,i=!0,r=!0,o,a,s){this._isNativeHtmlTable=n,this._stickCellCss=e,this._isBrowser=i,this._needsPositionStickyOnElement=r,this.direction=o,this._positionListener=a,this._tableInjector=s,this._borderCellCss={top:`${e}-border-elem-top`,bottom:`${e}-border-elem-bottom`,left:`${e}-border-elem-left`,right:`${e}-border-elem-right`}}clearStickyPositioning(n,e){(e.includes("left")||e.includes("right"))&&this._removeFromStickyColumnReplayQueue(n);let i=[];for(let r of n)r.nodeType===r.ELEMENT_NODE&&i.push(r,...Array.from(r.children));ht({write:()=>{for(let r of i)this._removeStickyStyle(r,e)}},{injector:this._tableInjector})}updateStickyColumns(n,e,i,r=!0,o=!0){if(!n.length||!this._isBrowser||!(e.some(R=>R)||i.some(R=>R))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let a=n[0],s=a.children.length,l=this.direction==="rtl",c=l?"right":"left",u=l?"left":"right",m=e.lastIndexOf(!0),v=i.indexOf(!0),_,y,I;o&&this._updateStickyColumnReplayQueue({rows:[...n],stickyStartStates:[...e],stickyEndStates:[...i]}),ht({earlyRead:()=>{_=this._getCellWidths(a,r),y=this._getStickyStartColumnPositions(_,e),I=this._getStickyEndColumnPositions(_,i)},write:()=>{for(let R of n)for(let V=0;V<s;V++){let _e=R.children[V];e[V]&&this._addStickyStyle(_e,c,y[V],V===m),i[V]&&this._addStickyStyle(_e,u,I[V],V===v)}this._positionListener&&_.some(R=>!!R)&&(this._positionListener.stickyColumnsUpdated({sizes:m===-1?[]:_.slice(0,m+1).map((R,V)=>e[V]?R:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:v===-1?[]:_.slice(v).map((R,V)=>i[V+v]?R:null).reverse()}))}},{injector:this._tableInjector})}stickRows(n,e,i){if(!this._isBrowser)return;let r=i==="bottom"?n.slice().reverse():n,o=i==="bottom"?e.slice().reverse():e,a=[],s=[],l=[];ht({earlyRead:()=>{for(let c=0,u=0;c<r.length;c++){if(!o[c])continue;a[c]=u;let m=r[c];l[c]=this._isNativeHtmlTable?Array.from(m.children):[m];let v=this._retrieveElementSize(m).height;u+=v,s[c]=v}},write:()=>{let c=o.lastIndexOf(!0);for(let u=0;u<r.length;u++){if(!o[u])continue;let m=a[u],v=u===c;for(let _ of l[u])this._addStickyStyle(_,i,m,v)}i==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:s,offsets:a,elements:l}):this._positionListener?.stickyFooterRowsUpdated({sizes:s,offsets:a,elements:l})}},{injector:this._tableInjector})}updateStickyFooterContainer(n,e){this._isNativeHtmlTable&&ht({write:()=>{let i=n.querySelector("tfoot");i&&(e.some(r=>!r)?this._removeStickyStyle(i,["bottom"]):this._addStickyStyle(i,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(n,e){if(!n.classList.contains(this._stickCellCss))return;for(let r of e)n.style[r]="",n.classList.remove(this._borderCellCss[r]);HR.some(r=>e.indexOf(r)===-1&&n.style[r])?n.style.zIndex=this._getCalculatedZIndex(n):(n.style.zIndex="",this._needsPositionStickyOnElement&&(n.style.position=""),n.classList.remove(this._stickCellCss))}_addStickyStyle(n,e,i,r){n.classList.add(this._stickCellCss),r&&n.classList.add(this._borderCellCss[e]),n.style[e]=`${i}px`,n.style.zIndex=this._getCalculatedZIndex(n),this._needsPositionStickyOnElement&&(n.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(n){let e={top:100,bottom:10,left:1,right:1},i=0;for(let r of HR)n.style[r]&&(i+=e[r]);return i?`${i}`:""}_getCellWidths(n,e=!0){if(!e&&this._cachedCellWidths.length)return this._cachedCellWidths;let i=[],r=n.children;for(let o=0;o<r.length;o++){let a=r[o];i.push(this._retrieveElementSize(a).width)}return this._cachedCellWidths=i,i}_getStickyStartColumnPositions(n,e){let i=[],r=0;for(let o=0;o<n.length;o++)e[o]&&(i[o]=r,r+=n[o]);return i}_getStickyEndColumnPositions(n,e){let i=[],r=0;for(let o=n.length;o>0;o--)e[o]&&(i[o]=r,r+=n[o]);return i}_retrieveElementSize(n){let e=this._elemSizeCache.get(n);if(e)return e;let i=n.getBoundingClientRect(),r={width:i.width,height:i.height};return this._resizeObserver&&(this._elemSizeCache.set(n,r),this._resizeObserver.observe(n,{box:"border-box"})),r}_updateStickyColumnReplayQueue(n){this._removeFromStickyColumnReplayQueue(n.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(n)}_removeFromStickyColumnReplayQueue(n){let e=new Set(n);for(let i of this._updatedStickyColumnsParamsToReplay)i.rows=i.rows.filter(r=>!e.has(r));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(i=>!!i.rows.length)}_updateCachedSizes(n){let e=!1;for(let i of n){let r=i.borderBoxSize?.length?{width:i.borderBoxSize[0].inlineSize,height:i.borderBoxSize[0].blockSize}:{width:i.contentRect.width,height:i.contentRect.height};r.width!==this._elemSizeCache.get(i.target)?.width&&O$(i.target)&&(e=!0),this._elemSizeCache.set(i.target,r)}e&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let i of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(i.rows,i.stickyStartStates,i.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function O$(t){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(n=>t.classList.contains(n))}var qd=new C("STICKY_POSITIONING_LISTENER");var B0=(()=>{class t{viewContainer=d(ct);elementRef=d(W);constructor(){let e=d(ir);e._rowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","rowOutlet",""]]})}return t})(),H0=(()=>{class t{viewContainer=d(ct);elementRef=d(W);constructor(){let e=d(ir);e._headerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","headerRowOutlet",""]]})}return t})(),U0=(()=>{class t{viewContainer=d(ct);elementRef=d(W);constructor(){let e=d(ir);e._footerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","footerRowOutlet",""]]})}return t})(),z0=(()=>{class t{viewContainer=d(ct);elementRef=d(W);constructor(){let e=d(ir);e._noDataRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","noDataRowOutlet",""]]})}return t})(),$0=(()=>{class t{_differs=d(Ur);_changeDetectorRef=d(Fe);_elementRef=d(W);_dir=d(Mt,{optional:!0});_platform=d(ke);_viewRepeater;_viewportRuler=d(Sr);_injector=d(oe);_virtualScrollViewport=d(xT,{optional:!0,host:!0});_positionListener=d(qd,{optional:!0})||d(qd,{optional:!0,skipSelf:!0});_document=d(j);_data;_renderedRange;_onDestroy=new k;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new k;_footerRowStickyUpdates=new k;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let e=this._elementRef.nativeElement.getAttribute("role");return e==="grid"||e==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(e){this._trackByFn=e}_trackByFn;get dataSource(){return this._dataSource}set dataSource(e){this._dataSource!==e&&(this._switchDataSource(e),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new k;_dataStream=new k;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(e){this._multiTemplateDataRows=e,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(e){this._fixedLayout=e,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new Z;viewChange=new Ct({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){d(new kn("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((i,r)=>this.trackBy?this.trackBy(r.dataIndex,r.data):r)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(Te(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new Ep:new sh,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(e=>{e?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),wp(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let e=this._dataDiffer.diff(this._renderRows);if(!e){this._updateNoDataRow(),this.contentChanged.next();return}let i=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(e,i,(r,o,a)=>this._getEmbeddedViewArgs(r.item,a),r=>r.item.data,r=>{r.operation===Yi.INSERTED&&r.context&&this._renderCellTemplateForItem(r.record.item.rowDef,r.context)}),this._updateRowIndexContext(),e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);o.context.$implicit=r.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(e){this._customColumnDefs.add(e)}removeColumnDef(e){this._customColumnDefs.delete(e)}addRowDef(e){this._customRowDefs.add(e)}removeRowDef(e){this._customRowDefs.delete(e)}addHeaderRowDef(e){this._customHeaderRowDefs.add(e),this._headerRowDefChanged=!0}removeHeaderRowDef(e){this._customHeaderRowDefs.delete(e),this._headerRowDefChanged=!0}addFooterRowDef(e){this._customFooterRowDefs.add(e),this._footerRowDefChanged=!0}removeFooterRowDef(e){this._customFooterRowDefs.delete(e),this._footerRowDefChanged=!0}setNoDataRow(e){this._customNoDataRow=e}updateStickyHeaderRowStyles(){let e=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let r=UR(this._headerRowOutlet,"thead");r&&(r.style.display=e.length?"":"none")}let i=this._headerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["top"]),this._stickyStyler.stickRows(e,i,"top"),this._headerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyFooterRowStyles(){let e=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let r=UR(this._footerRowOutlet,"tfoot");r&&(r.style.display=e.length?"":"none")}let i=this._footerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["bottom"]),this._stickyStyler.stickRows(e,i,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,i),this._footerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyColumnStyles(){let e=this._getRenderedRows(this._headerRowOutlet),i=this._getRenderedRows(this._rowOutlet),r=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...e,...i,...r],["left","right"]),this._stickyColumnStylesNeedReset=!1),e.forEach((o,a)=>{this._addStickyColumnStyles([o],this._headerRowDefs[a])}),this._rowDefs.forEach(o=>{let a=[];for(let s=0;s<i.length;s++)this._renderRows[s].rowDef===o&&a.push(i[s]);this._addStickyColumnStyles(a,o)}),r.forEach((o,a)=>{this._addStickyColumnStyles([o],this._footerRowDefs[a])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(e){this._positionListener?.stickyColumnsUpdated(e)}stickyEndColumnsUpdated(e){this._positionListener?.stickyEndColumnsUpdated(e)}stickyHeaderRowsUpdated(e){this._headerRowStickyUpdates.next(e),this._positionListener?.stickyHeaderRowsUpdated(e)}stickyFooterRowsUpdated(e){this._footerRowStickyUpdates.next(e),this._positionListener?.stickyFooterRowsUpdated(e)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let i=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||i,this._forceRecalculateCellWidths=i,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let e=[],i=Math.min(this._data.length,this._renderedRange.end),r=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<i;o++){let a=this._data[o],s=this._getRenderRowsForData(a,o,r.get(a));this._cachedRenderRowsMap.has(a)||this._cachedRenderRowsMap.set(a,new WeakMap);for(let l=0;l<s.length;l++){let c=s[l],u=this._cachedRenderRowsMap.get(c.data);u.has(c.rowDef)?u.get(c.rowDef).push(c):u.set(c.rowDef,[c]),e.push(c)}}return e}_getRenderRowsForData(e,i,r){return this._getRowDefs(e,i).map(a=>{let s=r&&r.has(a)?r.get(a):[];if(s.length){let l=s.shift();return l.dataIndex=i,l}else return{data:e,rowDef:a,dataIndex:i}})}_cacheColumnDefs(){this._columnDefsByName.clear(),ph(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(i=>{this._columnDefsByName.has(i.name),this._columnDefsByName.set(i.name,i)})}_cacheRowDefs(){this._headerRowDefs=ph(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=ph(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=ph(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let e=this._rowDefs.filter(i=>!i.when);this._defaultRowDef=e[0]}_renderUpdatedColumns(){let e=(a,s)=>{let l=!!s.getColumnsDiff();return a||l},i=this._rowDefs.reduce(e,!1);i&&this._forceRenderDataRows();let r=this._headerRowDefs.reduce(e,!1);r&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(e,!1);return o&&this._forceRenderFooterRows(),i||r||o}_switchDataSource(e){this._data=[],wp(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),e||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=e}_observeRenderChanges(){if(!this.dataSource)return;let e;wp(this.dataSource)?e=this.dataSource.connect(this):ia(this.dataSource)?e=this.dataSource:Array.isArray(this.dataSource)&&(e=J(this.dataSource)),this._renderChangeSubscription=ar([e,this.viewChange]).pipe(Te(this._onDestroy)).subscribe(([i,r])=>{this._data=i||[],this._renderedRange=r,this._dataStream.next(i),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((e,i)=>this._renderRow(this._headerRowOutlet,e,i)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((e,i)=>this._renderRow(this._footerRowOutlet,e,i)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(e,i){let r=Array.from(i?.columns||[]).map(s=>{let l=this._columnDefsByName.get(s);return l}),o=r.map(s=>s.sticky),a=r.map(s=>s.stickyEnd);this._stickyStyler.updateStickyColumns(e,o,a,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(e){let i=[];for(let r=0;r<e.viewContainer.length;r++){let o=e.viewContainer.get(r);i.push(o.rootNodes[0])}return i}_getRowDefs(e,i){if(this._rowDefs.length===1)return[this._rowDefs[0]];let r=[];if(this.multiTemplateDataRows)r=this._rowDefs.filter(o=>!o.when||o.when(i,e));else{let o=this._rowDefs.find(a=>a.when&&a.when(i,e))||this._defaultRowDef;o&&r.push(o)}return r.length,r}_getEmbeddedViewArgs(e,i){let r=e.rowDef,o={$implicit:e.data};return{templateRef:r.template,context:o,index:i}}_renderRow(e,i,r,o={}){let a=e.viewContainer.createEmbeddedView(i.template,o,r);return this._renderCellTemplateForItem(i,o),a}_renderCellTemplateForItem(e,i){for(let r of this._getCellTemplates(e))qa.mostRecentCellOutlet&&qa.mostRecentCellOutlet._viewContainer.createEmbeddedView(r,i);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let e=this._rowOutlet.viewContainer;for(let i=0,r=e.length;i<r;i++){let a=e.get(i).context;a.count=r,a.first=i===0,a.last=i===r-1,a.even=i%2===0,a.odd=!a.even,this.multiTemplateDataRows?(a.dataIndex=this._renderRows[i].dataIndex,a.renderIndex=i):a.index=this._renderRows[i].dataIndex}}_getCellTemplates(e){return!e||!e.columns?[]:Array.from(e.columns,i=>{let r=this._columnDefsByName.get(i);return e.extractCellTemplate(r)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let e=(i,r)=>i||r.hasStickyChanged();this._headerRowDefs.reduce(e,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(e,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(e,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let e=this._dir?this._dir.value:"ltr",i=this._injector;this._stickyStyler=new P0(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,e,this,i),(this._dir?this._dir.change:J()).pipe(Te(this._onDestroy)).subscribe(r=>{this._stickyStyler.direction=r,this.updateStickyColumnStyles()})}_setupVirtualScrolling(e){let i=typeof requestAnimationFrame<"u"?pu:uu;this.viewChange.next({start:0,end:0}),e.renderedRangeStream.pipe(rs(0,i),Te(this._onDestroy)).subscribe(this.viewChange),e.attach({dataStream:this._dataStream,measureRangeSize:(r,o)=>this._measureRangeSize(r,o)}),ar([e.renderedContentOffset,this._headerRowStickyUpdates]).pipe(Te(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let a=0;a<o.elements.length;a++){let s=o.elements[a];if(s){let l=o.offsets[a],c=r!==0?Math.max(r-l,l):-l;for(let u of s)u.style.top=`${-c}px`}}}),ar([e.renderedContentOffset,this._footerRowStickyUpdates]).pipe(Te(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let a=0;a<o.elements.length;a++){let s=o.elements[a];if(s)for(let l of s)l.style.bottom=`${r+o.offsets[a]}px`}})}_getOwnDefs(e){return e.filter(i=>!i._table||i._table===this)}_updateNoDataRow(){let e=this._customNoDataRow||this._noDataRow;if(!e)return;let i=this._rowOutlet.viewContainer.length===0;if(i===this._isShowingNoDataRow)return;let r=this._noDataRowOutlet.viewContainer;if(i){let o=r.createEmbeddedView(e.templateRef),a=o.rootNodes[0];if(o.rootNodes.length===1&&a?.nodeType===this._document.ELEMENT_NODE){a.setAttribute("role","row"),a.classList.add(...e._contentClassNames);let s=a.querySelectorAll(e._cellSelector);for(let l=0;l<s.length;l++)s[l].classList.add(...e._cellClassNames)}}else r.clear();this._isShowingNoDataRow=i,this._changeDetectorRef.markForCheck()}_measureRangeSize(e,i){if(e.start>=e.end||i!=="vertical")return 0;let r=this.viewChange.value,o=this._rowOutlet.viewContainer;e.start<r.start||e.end>r.end;let a=e.start-r.start,s=e.end-e.start,l,c;for(let v=0;v<s;v++){let _=o.get(v+a);if(_&&_.rootNodes.length){l=c=_.rootNodes[0];break}}for(let v=s-1;v>-1;v--){let _=o.get(v+a);if(_&&_.rootNodes.length){c=_.rootNodes[_.rootNodes.length-1];break}}let u=l?.getBoundingClientRect?.(),m=c?.getBoundingClientRect?.();return u&&m?m.bottom-u.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,WR,5)(o,Sl,5)(o,_h,5)(o,Yd,5)(o,L0,5),i&2){let a;Y(a=Q())&&(r._noDataRow=a.first),Y(a=Q())&&(r._contentColumnDefs=a),Y(a=Q())&&(r._contentRowDefs=a),Y(a=Q())&&(r._contentHeaderRowDefs=a),Y(a=Q())&&(r._contentFooterRowDefs=a)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(i,r){i&2&&X("cdk-table-fixed-layout",r.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",q],fixedLayout:[2,"fixedLayout","fixedLayout",q],recycleRows:[2,"recycleRows","recycleRows",q]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[we([{provide:ir,useExisting:t},{provide:qd,useValue:null}])],ngContentSelectors:T$,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(Me(I$),K(0),K(1,1),T(2,A$,1,0),T(3,R$,7,0)(4,k$,4,0)),i&2&&(h(2),A(r._isServer?2:-1),h(),A(r._isNativeHtmlTable?3:4))},dependencies:[H0,B0,z0,U0],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return t})();function ph(t,n){return t.concat(Array.from(n))}function UR(t,n){let e=n.toUpperCase(),i=t.viewContainer.element.nativeElement;for(;i;){let r=i.nodeType===1?i.nodeName:null;if(r===e)return i;if(r==="TABLE")break;i=i.parentNode}return null}var qR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[ld]})}return t})();var N$=["*"];function P$(t,n){t&1&&K(0)}var G0=(()=>{class t{_elementRef=d(W);constructor(){}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkStepHeader",""]],hostAttrs:["role","tab"]})}return t})(),W0=(()=>{class t{template=d(Ve);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkStepLabel",""]]})}return t})();var Ya={NUMBER:"number",EDIT:"edit",DONE:"done",ERROR:"error"},F$=new C("STEPPER_GLOBAL_OPTIONS"),yh=(()=>{class t{_stepperOptions;_stepper=d(Ml);_displayDefaultIndicatorType;stepLabel;_childForms;content;stepControl;get interacted(){return this._interacted()}set interacted(e){this._interacted.set(e)}_interacted=S(!1);interactedStream=new Z;label;errorMessage;ariaLabel;ariaLabelledby;get state(){return this._state()}set state(e){this._state.set(e)}_state=S(void 0);get editable(){return this._editable()}set editable(e){this._editable.set(e)}_editable=S(!0);optional=!1;get completed(){let e=this._completedOverride(),i=this._interacted();return e??(i&&(!this.stepControl||this.stepControl.valid))}set completed(e){this._completedOverride.set(e)}_completedOverride=S(null);index=S(-1);isSelected=Rt(()=>this._stepper.selectedIndex===this.index());indicatorType=Rt(()=>{let e=this.isSelected(),i=this.completed,r=this._state()??Ya.NUMBER,o=this._editable();return this._showError()&&this.hasError&&!e?Ya.ERROR:this._displayDefaultIndicatorType?!i||e?Ya.NUMBER:o?Ya.EDIT:Ya.DONE:i&&!e?Ya.DONE:i&&e?r:o&&e?Ya.EDIT:r});isNavigable=Rt(()=>{let e=this.isSelected();return this.completed||e||!this._stepper.linear});get hasError(){let e=this._customError();return e??this._getDefaultError()}set hasError(e){this._customError.set(e)}_customError=S(null);_getDefaultError(){return this.interacted&&!!this.stepControl?.invalid}constructor(){let e=d(F$,{optional:!0});this._stepperOptions=e||{},this._displayDefaultIndicatorType=this._stepperOptions.displayDefaultIndicatorType!==!1}select(){this._stepper.selected=this}reset(){this._interacted.set(!1),this._completedOverride()!=null&&this._completedOverride.set(!1),this._customError()!=null&&this._customError.set(!1),this.stepControl&&(this._childForms?.forEach(e=>e.resetForm?.()),this.stepControl.reset())}ngOnChanges(){this._stepper._stateChanged()}_markAsInteracted(){this._interacted()||(this._interacted.set(!0),this.interactedStream.emit(this))}_showError(){return this._stepperOptions.showError??this._customError()!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-step"]],contentQueries:function(i,r,o){if(i&1&&rt(o,W0,5)(o,Ki,5),i&2){let a;Y(a=Q())&&(r.stepLabel=a.first),Y(a=Q())&&(r._childForms=a)}},viewQuery:function(i,r){if(i&1&&dt(Ve,7),i&2){let o;Y(o=Q())&&(r.content=o.first)}},inputs:{stepControl:"stepControl",label:"label",errorMessage:"errorMessage",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],state:"state",editable:[2,"editable","editable",q],optional:[2,"optional","optional",q],completed:[2,"completed","completed",q],hasError:[2,"hasError","hasError",q]},outputs:{interactedStream:"interacted"},exportAs:["cdkStep"],features:[Pe],ngContentSelectors:N$,decls:1,vars:0,template:function(i,r){i&1&&(Me(),Os(0,P$,1,0,"ng-template"))},encapsulation:2,changeDetection:0})}return t})(),Ml=(()=>{class t{_dir=d(Mt,{optional:!0});_changeDetectorRef=d(Fe);_elementRef=d(W);_destroyed=new k;_keyManager;_steps;steps=new Xt;_stepHeader;_sortedHeaders=new Xt;get linear(){return this._linear()}set linear(e){this._linear.set(e)}_linear=S(!1);get selectedIndex(){return this._selectedIndex()}set selectedIndex(e){this._steps?(this._isValidIndex(e),this.selectedIndex!==e&&(this.selected?._markAsInteracted(),!this._anyControlsInvalidOrPending(e)&&(e>=this.selectedIndex||this.steps.toArray()[e].editable)&&this._updateSelectedItemIndex(e))):this._selectedIndex.set(e)}_selectedIndex=S(0);get selected(){return this.steps?this.steps.toArray()[this.selectedIndex]:void 0}set selected(e){this.selectedIndex=e&&this.steps?this.steps.toArray().indexOf(e):-1}selectionChange=new Z;selectedIndexChange=new Z;_groupId=d(Ue).getId("cdk-stepper-");get orientation(){return this._orientation}set orientation(e){this._orientation=e,this._keyManager&&this._keyManager.withVerticalOrientation(e==="vertical")}_orientation="horizontal";constructor(){}ngAfterContentInit(){this._steps.changes.pipe(We(this._steps),Te(this._destroyed)).subscribe(e=>{this.steps.reset(e.filter(i=>i._stepper===this)),this.steps.forEach((i,r)=>i.index.set(r)),this.steps.notifyOnChanges()})}ngAfterViewInit(){if(this._stepHeader.changes.pipe(We(this._stepHeader),Te(this._destroyed)).subscribe(e=>{this._sortedHeaders.reset(e.toArray().sort((i,r)=>i._elementRef.nativeElement.compareDocumentPosition(r._elementRef.nativeElement)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1)),this._sortedHeaders.notifyOnChanges()}),this._keyManager=new Xr(this._sortedHeaders).withWrap().withHomeAndEnd().withVerticalOrientation(this._orientation==="vertical"),this._keyManager.updateActiveItem(this.selectedIndex),(this._dir?this._dir.change:J()).pipe(We(this._layoutDirection()),Te(this._destroyed)).subscribe(e=>this._keyManager?.withHorizontalOrientation(e)),this._keyManager.updateActiveItem(this.selectedIndex),this.steps.changes.subscribe(()=>{this.selected||this._selectedIndex.set(Math.max(this.selectedIndex-1,0))}),this._isValidIndex(this.selectedIndex)||this._selectedIndex.set(0),this.linear&&this.selectedIndex>0){let e=this.steps.toArray().slice(0,this._selectedIndex());for(let i of e)i._markAsInteracted()}}ngOnDestroy(){this._keyManager?.destroy(),this.steps.destroy(),this._sortedHeaders.destroy(),this._destroyed.next(),this._destroyed.complete()}next(){this.selectedIndex=Math.min(this._selectedIndex()+1,this.steps.length-1)}previous(){this.selectedIndex=Math.max(this._selectedIndex()-1,0)}reset(){this._updateSelectedItemIndex(0),this.steps.forEach(e=>e.reset()),this._stateChanged()}_getStepLabelId(e){return`${this._groupId}-label-${e}`}_getStepContentId(e){return`${this._groupId}-content-${e}`}_stateChanged(){this._changeDetectorRef.markForCheck()}_getAnimationDirection(e){let i=e-this._selectedIndex();return i<0?this._layoutDirection()==="rtl"?"next":"previous":i>0?this._layoutDirection()==="rtl"?"previous":"next":"current"}_getFocusIndex(){return this._keyManager?this._keyManager.activeItemIndex:this._selectedIndex()}_updateSelectedItemIndex(e){let i=this.steps.toArray(),r=this._selectedIndex();this.selectionChange.emit({selectedIndex:e,previouslySelectedIndex:r,selectedStep:i[e],previouslySelectedStep:i[r]}),this._keyManager&&(this._containsFocus()?this._keyManager.setActiveItem(e):this._keyManager.updateActiveItem(e)),this._selectedIndex.set(e),this.selectedIndexChange.emit(e),this._stateChanged()}_onKeydown(e){let i=Ot(e),r=e.keyCode,o=this._keyManager;o?.activeItemIndex!=null&&!i&&(r===32||r===13)?(this.selectedIndex=o.activeItemIndex,e.preventDefault()):o?.setFocusOrigin("keyboard").onKeydown(e)}_anyControlsInvalidOrPending(e){return this.linear&&e>=0?this.steps.toArray().slice(0,e).some(i=>{let r=i.stepControl;return(r?r.invalid||r.pending||!i.interacted:!i.completed)&&!i.optional&&!i._completedOverride()}):!1}_layoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_containsFocus(){let e=this._elementRef.nativeElement,i=Pa();return e===i||e.contains(i)}_isValidIndex(e){return e>-1&&(!this.steps||e<this.steps.length)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkStepper",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,yh,5)(o,G0,5),i&2){let a;Y(a=Q())&&(r._steps=a),Y(a=Q())&&(r._stepHeader=a)}},inputs:{linear:[2,"linear","linear",q],selectedIndex:[2,"selectedIndex","selectedIndex",zi],selected:"selected",orientation:"orientation"},outputs:{selectionChange:"selectionChange",selectedIndexChange:"selectedIndexChange"},exportAs:["cdkStepper"]})}return t})(),YR=(()=>{class t{_stepper=d(Ml);type="submit";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["button","cdkStepperNext",""]],hostVars:1,hostBindings:function(i,r){i&1&&F("click",function(){return r._stepper.next()}),i&2&&xt("type",r.type)},inputs:{type:"type"}})}return t})(),QR=(()=>{class t{_stepper=d(Ml);type="button";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["button","cdkStepperPrevious",""]],hostVars:1,hostBindings:function(i,r){i&1&&F("click",function(){return r._stepper.previous()}),i&2&&xt("type",r.type)},inputs:{type:"type"}})}return t})(),ZR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var L$=(t,n,e)=>({index:t,active:n,optional:e});function V$(t,n){if(t&1&&it(0,2),t&2){let e=b();E("ngTemplateOutlet",e.iconOverrides[e.state])("ngTemplateOutletContext",ay(2,L$,e.index,e.active,e.optional))}}function j$(t,n){if(t&1&&(f(0,"span",7),g(1),p()),t&2){let e=b(2);h(),me(e._getDefaultTextForState(e.state))}}function B$(t,n){if(t&1&&(f(0,"span",8),g(1),p()),t&2){let e=b(3);h(),me(e._intl.completedLabel)}}function H$(t,n){if(t&1&&(f(0,"span",8),g(1),p()),t&2){let e=b(3);h(),me(e._intl.editableLabel)}}function U$(t,n){if(t&1&&(T(0,B$,2,1,"span",8)(1,H$,2,1,"span",8),f(2,"mat-icon",7),g(3),p()),t&2){let e=b(2);A(e.state==="done"?0:e.state==="edit"?1:-1),h(3),me(e._getDefaultTextForState(e.state))}}function z$(t,n){if(t&1&&T(0,j$,2,1,"span",7)(1,U$,4,2),t&2){let e,i=b();A((e=i.state)==="number"?0:1)}}function $$(t,n){t&1&&(f(0,"div",4),it(1,9),p()),t&2&&(h(),E("ngTemplateOutlet",n.template))}function G$(t,n){if(t&1&&(f(0,"div",4),g(1),p()),t&2){let e=b();h(),me(e.label)}}function W$(t,n){if(t&1&&(f(0,"div",5),g(1),p()),t&2){let e=b();h(),me(e._intl.optionalLabel)}}function q$(t,n){if(t&1&&(f(0,"div",6),g(1),p()),t&2){let e=b();h(),me(e.errorMessage)}}var KR=["*"];function Y$(t,n){}function Q$(t,n){if(t&1&&(K(0),N(1,Y$,0,0,"ng-template",0)),t&2){let e=b();h(),E("cdkPortalOutlet",e._portal)}}var Z$=["animatedContainer"],XR=t=>({steps:t}),JR=t=>({step:t});function K$(t,n){t&1&&K(0)}function X$(t,n){if(t&1&&(f(0,"div",5),it(1,9)(2,6),p()),t&2){let e=b(2),i=zt(6);h(),E("ngTemplateOutlet",e.headerPrefix()),h(),E("ngTemplateOutlet",i)("ngTemplateOutletContext",Ps(3,XR,e.steps))}}function J$(t,n){if(t&1&&it(0,6),t&2){let e=b(2),i=zt(6);E("ngTemplateOutlet",i)("ngTemplateOutletContext",Ps(2,XR,e.steps))}}function e3(t,n){if(t&1&&(f(0,"div",10,2),it(2,9),p()),t&2){let e=n.$implicit,i=n.$index,r=b(2);$t("mat-horizontal-stepper-content-"+r._getAnimationDirection(i)),E("id",r._getStepContentId(i)),de("aria-labelledby",r._getStepLabelId(i))("inert",r.selectedIndex===i?null:""),h(2),E("ngTemplateOutlet",e.content)}}function t3(t,n){if(t&1&&(f(0,"div",3),T(1,X$,3,5,"div",5)(2,J$,1,4,"ng-container",6),f(3,"div",7),Qe(4,e3,3,6,"div",8,Ye),p()()),t&2){let e=b();h(),A(e.headerPrefix()?1:2),h(3),Ze(e.steps)}}function n3(t,n){if(t&1&&it(0,9),t&2){let e=b(2);E("ngTemplateOutlet",e.headerPrefix())}}function i3(t,n){if(t&1&&(f(0,"div",11),it(1,6),f(2,"div",12,2)(4,"div",13)(5,"div",14),it(6,9),p()()()()),t&2){let e=n.$implicit,i=n.$index,r=n.$index,o=n.$count,a=b(2),s=zt(4);h(),E("ngTemplateOutlet",s)("ngTemplateOutletContext",Ps(11,JR,e)),h(),X("mat-stepper-vertical-line",r!==o-1)("mat-vertical-content-container-active",a.selectedIndex===i),de("inert",a.selectedIndex===i?null:"")("aria-label",a.ariaLabel),h(2),E("id",a._getStepContentId(i)),de("aria-labelledby",a._getStepLabelId(i)),h(2),E("ngTemplateOutlet",e.content)}}function r3(t,n){if(t&1&&(f(0,"div",4),T(1,n3,1,1,"ng-container",9),Qe(2,i3,7,13,"div",11,Ye),p()),t&2){let e=b();h(),A(e.headerPrefix()?1:-1),h(),Ze(e.steps)}}function o3(t,n){if(t&1){let e=Ke();f(0,"mat-step-header",15),F("click",function(){let r=Ae(e).step;return Re(r.select())})("keydown",function(r){Ae(e);let o=b();return Re(o._onKeydown(r))}),p()}if(t&2){let e=n.step,i=b();X("mat-horizontal-stepper-header",i.orientation==="horizontal")("mat-vertical-stepper-header",i.orientation==="vertical"),E("tabIndex",i._getFocusIndex()===e.index()?0:-1)("id",i._getStepLabelId(e.index()))("index",e.index())("state",e.indicatorType())("label",e.stepLabel||e.label)("selected",e.isSelected())("active",e.isNavigable())("optional",e.optional)("errorMessage",e.errorMessage)("iconOverrides",i._iconOverrides)("disableRipple",i.disableRipple||!e.isNavigable())("color",e.color||i.color),de("role",i.orientation==="horizontal"?"tab":"button")("aria-posinset",i.orientation==="horizontal"?e.index()+1:null)("aria-setsize",i.orientation==="horizontal"?i.steps.length:null)("aria-selected",i.orientation==="horizontal"?e.isSelected():null)("aria-current",i.orientation==="vertical"&&e.isSelected()?"step":null)("aria-disabled",i.orientation==="vertical"&&e.isSelected()?"true":null)("aria-expanded",i.orientation==="vertical"?e.isSelected():null)("aria-controls",i._getStepContentId(e.index()))("aria-label",e.ariaLabel||null)("aria-labelledby",!e.ariaLabel&&e.ariaLabelledby?e.ariaLabelledby:null)("aria-disabled",e.isNavigable()?null:!0)}}function a3(t,n){t&1&&z(0,"div",17)}function s3(t,n){if(t&1&&(it(0,6),T(1,a3,1,0,"div",17)),t&2){let e=n.$implicit,i=n.$index,r=n.$count;b(2);let o=zt(4);E("ngTemplateOutlet",o)("ngTemplateOutletContext",Ps(3,JR,e)),h(),A(i!==r-1?1:-1)}}function l3(t,n){if(t&1&&(f(0,"div",16),Qe(1,s3,2,5,null,null,Ye),p()),t&2){let e=n.steps,i=b();de("aria-label",i.ariaLabel),h(),Ze(e)}}var Qd=(()=>{class t extends W0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matStepLabel",""]],features:[ae]})}return t})(),c3=(()=>{class t{changes=new k;optionalLabel="Optional";completedLabel="Completed";editableLabel="Editable";static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),q0=(()=>{class t extends G0{_intl=d(c3);_focusMonitor=d(Pn);_intlSubscription;state;label;errorMessage;iconOverrides;index;selected=!1;active=!1;optional=!1;disableRipple=!1;color;constructor(){super();let e=d(_t);e.load(Ci),e.load(fl);let i=d(Fe);this._intlSubscription=this._intl.changes.subscribe(()=>i.markForCheck())}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){this._intlSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._elementRef)}focus(e,i){e?this._focusMonitor.focusVia(this._elementRef,e,i):this._elementRef.nativeElement.focus(i)}_stringLabel(){return this.label instanceof Qd?null:this.label}_templateLabel(){return this.label instanceof Qd?this.label:null}_getHostElement(){return this._elementRef.nativeElement}_getDefaultTextForState(e){return e=="number"?`${this.index+1}`:e=="edit"?"create":e=="error"?"warning":e}_hasEmptyLabel(){return!this._stringLabel()&&!this._templateLabel()&&!this._hasOptionalLabel()&&!this._hasErrorLabel()}_hasOptionalLabel(){return this.optional&&this.state!=="error"}_hasErrorLabel(){return this.state==="error"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-step-header"]],hostAttrs:["role","",1,"mat-step-header"],hostVars:4,hostBindings:function(i,r){i&2&&($t("mat-"+(r.color||"primary")),X("mat-step-header-empty-label",r._hasEmptyLabel()))},inputs:{state:"state",label:"label",errorMessage:"errorMessage",iconOverrides:"iconOverrides",index:"index",selected:"selected",active:"active",optional:"optional",disableRipple:"disableRipple",color:"color"},features:[ae],decls:10,vars:17,consts:[["matRipple","",1,"mat-step-header-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"],[1,"mat-step-icon-content"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-step-label"],[1,"mat-step-text-label"],[1,"mat-step-optional"],[1,"mat-step-sub-label-error"],["aria-hidden","true"],[1,"cdk-visually-hidden"],[3,"ngTemplateOutlet"]],template:function(i,r){if(i&1&&(z(0,"div",0),f(1,"div")(2,"div",1),T(3,V$,1,6,"ng-container",2)(4,z$,2,1),p()(),f(5,"div",3),T(6,$$,2,1,"div",4)(7,G$,2,1,"div",4),T(8,W$,2,1,"div",5),T(9,q$,2,1,"div",6),p()),i&2){let o;E("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disableRipple),h(),$t(oy("mat-step-icon-state-",r.state," mat-step-icon")),X("mat-step-icon-selected",r.selected),h(2),A(r.iconOverrides&&r.iconOverrides[r.state]?3:4),h(2),X("mat-step-label-active",r.active)("mat-step-label-selected",r.selected)("mat-step-label-error",r.state=="error"),h(),A((o=r._templateLabel())?6:r._stringLabel()?7:-1,o),h(2),A(r._hasOptionalLabel()?8:-1),h(),A(r._hasErrorLabel()?9:-1)}},dependencies:[jo,Bs,Bn],styles:[`.mat-step-header {
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
`],encapsulation:2,changeDetection:0})}return t})(),d3=(()=>{class t{templateRef=d(Ve);name;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","matStepperIcon",""]],inputs:{name:[0,"matStepperIcon","name"]}})}return t})(),u3=(()=>{class t{_template=d(Ve);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["ng-template","matStepContent",""]]})}return t})(),Y0=(()=>{class t extends yh{_errorStateMatcher=d($o,{skipSelf:!0});_viewContainerRef=d(ct);_isSelected=pe.EMPTY;stepLabel=void 0;color;_lazyContent;_portal;ngAfterContentInit(){this._isSelected=this._stepper.steps.changes.pipe(mt(()=>this._stepper.selectionChange.pipe(he(e=>e.selectedStep===this),We(this._stepper.selected===this)))).subscribe(e=>{e&&this._lazyContent&&!this._portal&&(this._portal=new vn(this._lazyContent._template,this._viewContainerRef))})}ngOnDestroy(){this._isSelected.unsubscribe()}isErrorState(e,i){let r=this._errorStateMatcher.isErrorState(e,i),o=!!(e&&e.invalid&&this.interacted);return r||o}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-step"]],contentQueries:function(i,r,o){if(i&1&&rt(o,Qd,5)(o,u3,5),i&2){let a;Y(a=Q())&&(r.stepLabel=a.first),Y(a=Q())&&(r._lazyContent=a.first)}},hostAttrs:["hidden",""],inputs:{color:"color"},exportAs:["matStep"],features:[we([{provide:$o,useExisting:t},{provide:yh,useExisting:t}]),ae],ngContentSelectors:KR,decls:1,vars:0,consts:[[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Me(),N(0,Q$,2,1,"ng-template"))},dependencies:[vi],encapsulation:2,changeDetection:0})}return t})(),Q0=(()=>{class t extends Ml{_ngZone=d(re);_renderer=d(qe);_animationsDisabled=tt();_cleanupTransition;_isAnimating=S(!1);_stepHeader=void 0;_animatedContainers;_steps=void 0;steps=new Xt;_icons;animationDone=new Z;disableRipple=!1;color;labelPosition="end";headerPosition="top";ariaLabel=null;headerPrefix=Tm(null);_iconOverrides={};get animationDuration(){return this._animationDuration}set animationDuration(e){this._animationDuration=/^\d+$/.test(e)?e+"ms":e}_animationDuration="";_isServer=!d(ke).isBrowser;constructor(){super();let i=d(W).nativeElement.nodeName.toLowerCase();this.orientation=i==="mat-vertical-stepper"?"vertical":"horizontal"}ngAfterContentInit(){super.ngAfterContentInit(),this._icons.forEach(({name:e,templateRef:i})=>this._iconOverrides[e]=i),this.steps.changes.pipe(Te(this._destroyed)).subscribe(()=>this._stateChanged()),this.selectedIndexChange.pipe(Te(this._destroyed)).subscribe(()=>{let e=this._getAnimationDuration();e==="0ms"||e==="0s"?this._onAnimationDone():this._isAnimating.set(!0)}),this._ngZone.runOutsideAngular(()=>{this._animationsDisabled||setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-stepper-animations-enabled"),this._cleanupTransition=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionend)},200)})}ngAfterViewInit(){if(super.ngAfterViewInit(),typeof queueMicrotask=="function"){let e=!1;this._animatedContainers.changes.pipe(We(null),Te(this._destroyed)).subscribe(()=>queueMicrotask(()=>{e||(e=!0,this.animationDone.emit()),this._stateChanged()}))}}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransition?.()}_getAnimationDuration(){return this._animationsDisabled?"0ms":this.animationDuration?this.animationDuration:this.orientation==="horizontal"?"500ms":"225ms"}_handleTransitionend=e=>{let i=e.target;if(!i)return;let r=this.orientation==="horizontal"&&e.propertyName==="transform"&&i.classList.contains("mat-horizontal-stepper-content-current"),o=this.orientation==="vertical"&&e.propertyName==="grid-template-rows"&&i.classList.contains("mat-vertical-content-container-active");(r||o)&&this._animatedContainers.find(s=>s.nativeElement===i)&&this._onAnimationDone()};_onAnimationDone(){this._isAnimating.set(!1),this.animationDone.emit()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-stepper"],["mat-vertical-stepper"],["mat-horizontal-stepper"],["","matStepper",""]],contentQueries:function(i,r,o){if(i&1&&rt(o,Y0,5)(o,d3,5),i&2){let a;Y(a=Q())&&(r._steps=a),Y(a=Q())&&(r._icons=a)}},viewQuery:function(i,r){if(i&1&&dt(q0,5)(Z$,5),i&2){let o;Y(o=Q())&&(r._stepHeader=o),Y(o=Q())&&(r._animatedContainers=o)}},hostVars:14,hostBindings:function(i,r){i&2&&(x("--mat-stepper-animation-duration",r._getAnimationDuration()),X("mat-stepper-horizontal",r.orientation==="horizontal")("mat-stepper-vertical",r.orientation==="vertical")("mat-stepper-label-position-end",r.orientation==="horizontal"&&r.labelPosition=="end")("mat-stepper-label-position-bottom",r.orientation==="horizontal"&&r.labelPosition=="bottom")("mat-stepper-header-position-bottom",r.headerPosition==="bottom")("mat-stepper-animating",r._isAnimating()))},inputs:{disableRipple:"disableRipple",color:"color",labelPosition:"labelPosition",headerPosition:"headerPosition",ariaLabel:[0,"aria-label","ariaLabel"],headerPrefix:[1,"headerPrefix"],animationDuration:"animationDuration"},outputs:{animationDone:"animationDone"},exportAs:["matStepper","matVerticalStepper","matHorizontalStepper"],features:[we([{provide:Ml,useExisting:t}]),ae],ngContentSelectors:KR,decls:7,vars:2,consts:[["stepTemplate",""],["horizontalStepsTemplate",""],["animatedContainer",""],[1,"mat-horizontal-stepper-wrapper"],[1,"mat-vertical-stepper-wrapper"],[1,"mat-horizontal-stepper-header-wrapper"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-horizontal-content-container"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id","class"],[3,"ngTemplateOutlet"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id"],[1,"mat-step"],[1,"mat-vertical-content-container"],["role","region",1,"mat-vertical-stepper-content",3,"id"],[1,"mat-vertical-content"],[3,"click","keydown","tabIndex","id","index","state","label","selected","active","optional","errorMessage","iconOverrides","disableRipple","color"],["aria-orientation","horizontal","role","tablist",1,"mat-horizontal-stepper-header-container"],[1,"mat-stepper-horizontal-line"]],template:function(i,r){if(i&1&&(Me(),T(0,K$,1,0),T(1,t3,6,1,"div",3)(2,r3,4,1,"div",4),N(3,o3,1,27,"ng-template",null,0,Ls)(5,l3,3,1,"ng-template",null,1,Ls)),i&2){let o;A(r._isServer?0:-1),h(),A((o=r.orientation)==="horizontal"?1:o==="vertical"?2:-1)}},dependencies:[Bs,q0],styles:[`.mat-stepper-vertical,
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
`],encapsulation:2,changeDetection:0})}return t})(),ek=(()=>{class t extends YR{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["button","matStepperNext",""]],hostAttrs:[1,"mat-stepper-next"],hostVars:1,hostBindings:function(i,r){i&2&&xt("type",r.type)},features:[ae]})}return t})(),tk=(()=>{class t extends QR{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["button","matStepperPrevious",""]],hostAttrs:[1,"mat-stepper-previous"],hostVars:1,hostBindings:function(i,r){i&2&&xt("type",r.type)},features:[ae]})}return t})(),nk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({providers:[$o],imports:[_i,ZR,Wo,Bo,Q0,q0,Ce]})}return t})();var m3=[[["caption"]],[["colgroup"],["col"]],"*"],p3=["caption","colgroup, col","*"];function h3(t,n){t&1&&K(0,2)}function g3(t,n){t&1&&(f(0,"thead",0),it(1,1),p(),f(2,"tbody",2),it(3,3)(4,4),p(),f(5,"tfoot",0),it(6,5),p())}function v3(t,n){t&1&&it(0,1)(1,3)(2,4)(3,5)}var bn=(()=>{class t extends $0{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(i,r){i&2&&X("mat-table-fixed-layout",r.fixedLayout)},exportAs:["matTable"],features:[we([{provide:$0,useExisting:t},{provide:ir,useExisting:t},{provide:qd,useValue:null}]),ae],ngContentSelectors:p3,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(Me(m3),K(0),K(1,1),T(2,h3,1,0),T(3,g3,7,0)(4,v3,4,0)),i&2&&(h(2),A(r._isServer?2:-1),h(),A(r._isNativeHtmlTable?3:4))},dependencies:[H0,B0,z0,U0],styles:[`.mat-mdc-table-sticky {
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
`],encapsulation:2})}return t})(),Cn=(()=>{class t extends gh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matCellDef",""]],features:[we([{provide:gh,useExisting:t}]),ae]})}return t})(),Dn=(()=>{class t extends vh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matHeaderCellDef",""]],features:[we([{provide:vh,useExisting:t}]),ae]})}return t})();var wn=(()=>{class t extends Sl{get name(){return this._name}set name(e){this._setNameInput(e)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[we([{provide:Sl,useExisting:t}]),ae]})}return t})(),En=(()=>{class t extends $R{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[ae]})}return t})();var xn=(()=>{class t extends GR{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[ae]})}return t})();var Sn=(()=>{class t extends Yd{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",q]},features:[we([{provide:Yd,useExisting:t}]),ae]})}return t})();var Mn=(()=>{class t extends _h{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[we([{provide:_h,useExisting:t}]),ae]})}return t})(),In=(()=>{class t extends V0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[we([{provide:V0,useExisting:t}]),ae],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[qa],encapsulation:2})}return t})();var Tn=(()=>{class t extends j0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=je(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[we([{provide:j0,useExisting:t}]),ae],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&it(0,0)},dependencies:[qa],encapsulation:2})}return t})();var Z0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[qR,Ce]})}return t})();var _3=["*",[["mat-toolbar-row"]]],y3=["*","mat-toolbar-row"],b3=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),ik=(()=>{class t{_elementRef=d(W);_platform=d(ke);_document=d(j);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&rt(o,b3,5),i&2){let a;Y(a=Q())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&($t(r.color?"mat-"+r.color:""),X("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:y3,decls:2,vars:0,template:function(i,r){i&1&&(Me(_3),K(0),K(1,1))},styles:[`.mat-toolbar {
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
`],encapsulation:2,changeDetection:0})}return t})();var rk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ce]})}return t})();var Si=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[Ho,Wo,Yb,b0,Go,I0,O0,T0,h0,Sa]})},Mi=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[lh,Ho,Wo,Z0,Sa]})},bh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[lh,Ho,Wo,b0,Go,I0,O0,T0,h0,Sa]})},Ch=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[rk,Wo,jR,BR,iR]})},qo=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=U({type:t});static \u0275inj=B({imports:[lh,Ho,Wo,Z0,Sa,Yb]})};var ve=class t{static snackBarConfig(){let n=new Uo;return n.duration=12e3,n.panelClass=["btn","btn-outline-dark"],n.verticalPosition="top",n.horizontalPosition="center",n}static openSnackBar(n,e,i){let r=t.snackBarConfig();i.open(n,e,r)}static getMatDialogConf(){let n=new pl;return n.disableClose=!1,n.autoFocus=!0,n.exitAnimationDuration="1000ms",n.enterAnimationDuration="1000ms",n.width="90vh",n.height="80vh",n.maxWidth="100vh",n}static async generateSHA256(n){var e=new TextEncoder;let i=e.encode(n),r=await crypto.subtle.digest("SHA-256",i);return Array.from(new Uint8Array(r)).map(a=>a.toString(16).padStart(2,"0")).join("")}static isSha256(n){return/^[0-9a-fA-F]{64}$/.test(n)}};var L=class{static PRODUCTOS_ID="productoList";static USUARIOS_ID="usuarioList";static LOTES_INVENTARIO_ID="loteList";static PEDIDOS_ID="pedidoList";static DETALLE_PEDIDOS_ID="detallePedidoList";static MOVIMIENTOS_INVENTARIO_ID="movimientoInventarioList";static ESTADO_PEDIDO_ID="estadoPedidoList";static ESTADO_PRODUCTO_ID="estadoProductoList";static TIPO_MOVIMIENTO_ID="tipoMovimientoList";static PASARELA_ID="pasarelaList";static DETALLE_PEDIDOS_COMPRA_ID="detallePedidoCompraList";static LOGGED_USUARIO="usuario";static LOGGED="logged";static estadoPedidoVacio(){return{id:0,descripcion:""}}static estadoProductoVacio(){return{id:0,descripcion:""}}static tipoMovimientoVacio(){return{id:0,descripcion:"",cuenta:""}}static usuarioVacio(){return{id:0,usuario:"",password:"",nombres:"",apellidos:"",fecha_creacion:new Date}}static productoVacio(){return{id:0,sku:"",nombre:"",descripcion:"",precio_venta:0,estado:this.estadoProductoVacio()}}static loteInventarioVacio(){return{id:0,producto:this.productoVacio(),cantidad_inicial:0,cantidad_actual:0,costo_unitario:0,fecha_ingreso:new Date}}static pedidoVacio(){return{id:0,usuario:this.usuarioVacio(),total:0,estado:this.estadoPedidoVacio()}}static detallePedidoVacio(){return{id:0,pedido:this.pedidoVacio(),producto:this.productoVacio(),cantidad:0,precio_unitario_venta:0}}static detalleCompraVacio(){return{id:0,producto:this.productoVacio(),cantidad:0,cantidad_local:0,cantidad_web:0,precio_unitario_venta:0}}static movimientoInventarioVacio(){return{id:0,producto:this.productoVacio(),lote:this.loteInventarioVacio(),tipo_movimiento:this.tipoMovimientoVacio(),cantidad:0,pedido:this.pedidoVacio(),fecha:new Date}}static pasarelaVacio(){return{id:0,nombre:"",descripcion:"",comision:0}}static getUsuarios(){var n=[],e="";return ve.generateSHA256("1234").then(i=>e=i),n.push({id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}),n.push({id:2,usuario:"jamileth",password:e,nombres:"Jamileth",apellidos:"Martinez",fecha_creacion:new Date}),n.push({id:3,usuario:"rubix",password:e,nombres:"Rubi",apellidos:"Mejia",fecha_creacion:new Date}),n.push({id:4,usuario:"ale",password:e,nombres:"Alejandra",apellidos:"Guardado",fecha_creacion:new Date}),n.push({id:5,usuario:"khaysernberg",password:e,nombres:"Cesar",apellidos:"Gomez",fecha_creacion:new Date}),n.push({id:6,usuario:"miriam",password:e,nombres:"Mirian",apellidos:"Rivas",fecha_creacion:new Date}),n}static getEstadosPedido(){var n=[];return n.push({id:1,descripcion:"Carrito"}),n.push({id:2,descripcion:"Apartado"}),n.push({id:3,descripcion:"Pagado"}),n}static getEstadosProducto(){var n=[];return n.push({id:1,descripcion:"Disponible"}),n.push({id:2,descripcion:"NoDisponible"}),n.push({id:3,descripcion:"Agotado"}),n}static getTiposMovimiento(){var n=[];return n.push({id:1,descripcion:"Compra",cuenta:"410101"}),n.push({id:2,descripcion:"Venta",cuenta:"210801"}),n.push({id:3,descripcion:"Merma",cuenta:"420201"}),n.push({id:4,descripcion:"Devolucion Compra",cuenta:"520403"}),n.push({id:5,descripcion:"Devolucion Venta",cuenta:"510103"}),n}static getProductos(){var n=[],e=this.getEstadosProducto();return n.push({id:1,sku:"p1",nombre:"producto1",descripcion:"descripcion1",precio_venta:1.5,estado:e[0],stock_local:25,stock_web:125}),n.push({id:2,sku:"p2",nombre:"producto2",descripcion:"descripcion2",precio_venta:2.5,estado:e[0],stock_local:25,stock_web:50}),n.push({id:3,sku:"p3",nombre:"producto3",descripcion:"descripcion3",precio_venta:3.5,estado:e[0],stock_local:25,stock_web:125}),n}static getLotesInventario(){var n=[],e=this.getProductos();return n.push({id:1,producto:e[0],cantidad_inicial:100,cantidad_actual:100,costo_unitario:.9,fecha_ingreso:new Date}),n.push({id:2,producto:e[0],cantidad_inicial:50,cantidad_actual:50,costo_unitario:.95,fecha_ingreso:new Date}),n.push({id:3,producto:e[1],cantidad_inicial:75,cantidad_actual:75,costo_unitario:2,fecha_ingreso:new Date}),n.push({id:4,producto:e[2],cantidad_inicial:150,cantidad_actual:150,costo_unitario:2.75,fecha_ingreso:new Date}),n}static getPedidos(){var n=[],e=this.getUsuarios(),i=this.getEstadosPedido();return n.push({id:1,usuario:e[1],total:10,estado:i[0]}),n.push({id:2,usuario:e[2],total:10,estado:i[0]}),n.push({id:3,usuario:e[3],total:10,estado:i[2]}),n}static getDetallePedidos(){var n=[],e=this.getProductos(),i=this.getPedidos();return n.push({id:1,pedido:i[0],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:2,pedido:i[0],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:3,pedido:i[1],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:4,pedido:i[1],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:5,pedido:i[2],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:6,pedido:i[2],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n}static getMovimientosInventario(){var n=[],e=this.getProductos(),i=this.getLotesInventario(),r=this.getTiposMovimiento();return n.push({id:1,producto:e[0],lote:i[0],tipo_movimiento:r[0],cantidad:100,fecha:new Date}),n.push({id:2,producto:e[0],lote:i[1],tipo_movimiento:r[0],cantidad:50,fecha:new Date}),n.push({id:3,producto:e[1],lote:i[2],tipo_movimiento:r[0],cantidad:75,fecha:new Date}),n.push({id:4,producto:e[2],lote:i[3],tipo_movimiento:r[0],cantidad:150,fecha:new Date}),n}static getPasarelas(){var n=[];return n.push({id:1,nombre:"Wompi",descripcion:"wompi el salvador",comision:1.5}),n.push({id:2,nombre:"Serfinsa",descripcion:"serfinsa el salvador",comision:1.5}),n.push({id:3,nombre:"PayPal",descripcion:"PayPal international",comision:1.5}),n}};var D3=new C("WindowLocalStorage",{providedIn:"root",factory:()=>{let t=d(jr);return Bm(t)?localStorage:{length:0,clear:()=>{},getItem:()=>null,key:()=>null,removeItem:()=>{},setItem:()=>{}}}}),on=class t{storage=d(D3);setItem(n,e){let i=typeof e=="string"?e:JSON.stringify(e);this.storage.setItem(n,i)}getItem(n){let e=this.storage.getItem(n);if(!e)return null;try{return JSON.parse(e)}catch{return e}}removeItem(n){this.storage.removeItem(n)}clear(){this.storage.clear()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Je=class t{localStorageService=d(on);constructor(){}getUsuarios(){var n=[],e=this.localStorageService.getItem(L.USUARIOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getUsuarios(),this.localStorageService.setItem(L.USUARIOS_ID,n)),n}getEstadosPedido(){var n=[],e=this.localStorageService.getItem(L.ESTADO_PEDIDO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getEstadosPedido(),this.localStorageService.setItem(L.ESTADO_PEDIDO_ID,n)),n}getEstadosProducto(){var n=[],e=this.localStorageService.getItem(L.ESTADO_PRODUCTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getEstadosProducto(),this.localStorageService.setItem(L.ESTADO_PRODUCTO_ID,n)),n}getTiposMovimiento(){var n=[],e=this.localStorageService.getItem(L.TIPO_MOVIMIENTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getTiposMovimiento(),this.localStorageService.setItem(L.TIPO_MOVIMIENTO_ID,n)),n}getProductos(){var n=[],e=this.localStorageService.getItem(L.PRODUCTOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getProductos(),this.localStorageService.setItem(L.PRODUCTOS_ID,n)),n}getLotesInventario(){var n=[],e=this.localStorageService.getItem(L.LOTES_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getLotesInventario(),this.localStorageService.setItem(L.LOTES_INVENTARIO_ID,n)),n}getPedidos(){var n=[],e=this.localStorageService.getItem(L.PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getPedidos(),this.localStorageService.setItem(L.PEDIDOS_ID,n)),n}getDetallePedidos(){var n=[],e=this.localStorageService.getItem(L.DETALLE_PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getDetallePedidos(),this.localStorageService.setItem(L.DETALLE_PEDIDOS_ID,n)),n}getMovimientosInventario(){var n=[],e=this.localStorageService.getItem(L.MOVIMIENTOS_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getMovimientosInventario(),this.localStorageService.setItem(L.MOVIMIENTOS_INVENTARIO_ID,n)),n}getPasarelas(){var n=[],e=this.localStorageService.getItem(L.PASARELA_ID);return e&&(n=e,this.localStorageService.setItem(L.PASARELA_ID,n)),(!n||n.length==0)&&(n=L.getPasarelas()),n}pushProducto(n){var e=this.getProductos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.PRODUCTOS_ID,e)}pushLoteInventario(n){var e=this.getLotesInventario();return n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.LOTES_INVENTARIO_ID,e),n}pushPedido(n){var e=this.getPedidos();return n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.PEDIDOS_ID,e),n}pushDetallePedido(n){var e=this.getDetallePedidos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.DETALLE_PEDIDOS_ID,e)}pushMovimientoInventario(n){var e=this.getMovimientosInventario();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.MOVIMIENTOS_INVENTARIO_ID,e)}editarProducto(n){var e=this.getProductos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.PRODUCTOS_ID,i)}editarLoteInventario(n){var e=this.getLotesInventario(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.LOTES_INVENTARIO_ID,i)}editarDetallePedido(n){var e=this.getDetallePedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.DETALLE_PEDIDOS_ID,i)}editarPedido(n){var e=this.getPedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.PEDIDOS_ID,i)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Ii=class t{dataService=d(Je);_snackBar=d(qt);constructor(){}getPedidosUsuario(n){var e=[],i=this.dataService.getPedidos();if(i.length>0)for(let r of i)r.usuario.id==n.id&&r.estado.id<3&&e.push(r);return e}getUltimoPedidoUsuario(n){var e=L.pedidoVacio(),i=this.getPedidosUsuario(n);if(i.length>0)e=i[i.length-1];else{let r=this.dataService.getEstadosPedido();e.usuario=n,e.total=0,e.estado=r[r.findIndex(o=>o.id==1)],e=this.dataService.pushPedido(e)}return e}getDetallesPedido(n){var e=[],i=this.dataService.getDetallePedidos();if(i.length>0)for(let r of i)r.pedido.id==n.id&&e.push(r);return e}aumentarDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&(i.cantidad++,this.dataService.editarDetallePedido(i));return e}disminuirDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&i.cantidad>0&&(i.cantidad--,this.dataService.editarDetallePedido(i));return e}pedidoPagado(n){var e=this.dataService.getEstadosPedido();n.estado=e[e.findIndex(r=>r.id==3)],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)r.pedido.id==n.id&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r,1))}ventaPagada(n){var e=this.dataService.getEstadosPedido();n.estado=e[e.findIndex(r=>r.id==3)],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)r.pedido.id==n.id&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r,2))}registrarPagoInventario(n,e){let i=L.movimientoInventarioVacio();i.cantidad=n.cantidad,i.fecha=new Date,i.pedido=n.pedido,i.producto=n.producto;let r=this.dataService.getTiposMovimiento();i.tipo_movimiento=r[r.findIndex(l=>l.id==2)];let o=this.dataService.getLotesInventario(),a=[],s=0;s=n.cantidad;for(let l of o)l.producto.id==n.producto.id&&l.cantidad_actual>0&&s>0&&(a.push(l),l.cantidad_actual<s&&(s-=l.cantidad_actual));s=n.cantidad;for(let l of a)if(s>0){l.cantidad_actual<s?(s-=l.cantidad_actual,l.cantidad_actual=0):(l.cantidad_actual-=s,s=0);let c=i;c.lote=l,this.dataService.editarLoteInventario(l),this.dataService.pushMovimientoInventario(c)}e==1&&n.producto.stock_web&&n.producto.stock_web>0&&(n.producto.stock_web-=n.cantidad,this.dataService.editarProducto(n.producto)),e==2&&n.producto.stock_local&&n.producto.stock_local>0&&(n.producto.stock_local-=n.cantidad,this.dataService.editarProducto(n.producto))}validarExistenciaInventario(n){let e=this.dataService.getLotesInventario(),i=[],r=0;r=n.cantidad;for(let o of e)o.producto.id==n.producto.id&&o.cantidad_actual>0&&r>0&&(i.push(o),o.cantidad_actual<r?r-=o.cantidad_actual:r=0);return i&&i.length>0&&r==0}getProductosStocWeb(){let n=this.dataService.getProductos(),e=[];if(n.length>0)for(let i of n)i.estado.id==1&&i.stock_web&&i.stock_web>0&&e.push(i);return e}getProductosStocLocal(){let n=this.dataService.getProductos(),e=[];if(n.length>0)for(let i of n)i.estado.id==1&&i.stock_local&&i.stock_local>0&&e.push(i);return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Hn=class t{formBuilder=d(th);validateFormControls(n){var e=n.valid,i=n.getRawValue();for(let r=0;r<i.length;r++)i[r]?.enabled&&!i[r]?.valid&&(e=!1);return e}getFormGroup(n){var e={};return n&&n.length>0&&n.forEach(i=>e[i.name]=[i.value||"",i.validators||[]]),this.formBuilder.group(e)}newProductoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.descripcion,value:i})),[{id:1,name:"sku",label:"Codigo",type:"text",controlType:"input",validators:[H.required,H.minLength(3)]},{id:2,name:"nombre",label:"Nombre Producto",type:"text",controlType:"input",validators:[H.required,H.minLength(5)]},{id:3,name:"descripcion",label:"Descripcion",type:"text",controlType:"input",validators:[H.required,H.minLength(5)]},{id:4,name:"precio_venta",value:0,label:"Precio de Venta",type:"number",controlType:"input",validators:[H.required,H.min(.01)]},{id:5,name:"estado",label:"Estado",type:"text",controlType:"select",validators:[H.required],options:e}]}newCompraFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[H.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[H.required,H.min(.01)]},{id:3,name:"pagoTotal",value:0,label:"Pago Total",type:"number",controlType:"input",validators:[H.required,H.min(.01)]}]}newVentaLineaFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[H.required],options:e}]}newVentaLocalFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[H.required],options:e}]}newVentaLocalTipoPagoFormControls(){var n=[];return n.push({id:1,name:"Efectivo",value:1}),n.push({id:2,name:"Tarjeta",value:2}),[{id:1,name:"tipoPago",label:"Forma de Pago",type:"text",controlType:"select",validators:[H.required],options:n}]}newVentaLocalClienteFormControls(){return[{id:1,name:"nombre",label:"Nombre",type:"text",controlType:"input",validators:[H.required,H.minLength(3)]},{id:2,name:"documento",label:"Numero Documento",type:"text",controlType:"input",validators:[H.required,H.minLength(3)]}]}newAgregarDetallePedidoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", Precio: $"+i.precio_venta,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[H.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[H.required,H.min(1)]}]}newAgregarDetallePedidoVentaControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", Disponible: $"+i.stock_local,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[H.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[H.required,H.min(1)]}]}newAgregarDetalleCompraControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[H.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[H.required,H.min(1)]},{id:3,name:"cantidad_web",value:0,label:"Cantidad Stock Web",type:"number",controlType:"input",validators:[H.required,H.min(0)]},{id:4,name:"cantidad_local",value:0,label:"Cantidad Stock local",type:"number",controlType:"input",validators:[H.required,H.min(0)]}]}newMermaForm(){return this.formBuilder.group({sku:["",[H.required,H.minLength(5)]],nombre:["",[H.required,H.minLength(3)]],descripcion:["",[H.required,H.minLength(3)]],precio_venta:[0,[H.required,H.min(.01)]],estado:[L.estadoProductoVacio(),[H.required]]})}newDevolucionCompraForm(){return this.formBuilder.group({sku:["",[H.required,H.minLength(5)]],nombre:["",[H.required,H.minLength(3)]],descripcion:["",[H.required,H.minLength(3)]],precio_venta:[0,[H.required,H.min(.01)]],estado:[L.estadoProductoVacio(),[H.required]]})}newDevolucionVentaForm(){return this.formBuilder.group({sku:["",[H.required,H.minLength(5)]],nombre:["",[H.required,H.minLength(3)]],descripcion:["",[H.required,H.minLength(3)]],precio_venta:[0,[H.required,H.min(.01)]],estado:[L.estadoProductoVacio(),[H.required]]})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Un=class t{dataService=d(Je);localStorageService=d(on);constructor(){}getUsuarioAnonimo(){return{id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}}getUsuarios(){return this.dataService.getUsuarios()}pushUsuario(n){var e=this.getUsuarios();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.USUARIOS_ID,e)}editarUsuario(n){var e=this.getUsuarios(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n,ve.isSha256(o.password)||ve.generateSHA256(o.password).then(a=>o.password=a)),i.push(o)}),this.localStorageService.setItem(L.USUARIOS_ID,i)}validarUsuario(n,e){var i="";ve.generateSHA256(e).then(a=>i=a);var r=this.getUsuarios(),o=L.usuarioVacio();return r&&r.length&&r.filter(a=>a.usuario==n).filter(a=>a.password==i).forEach(a=>o=a),o&&o.id>1?o:null}getUsuarioLoggeado(){var n=L.usuarioVacio(),e=this.localStorageService.getItem("usuario");return e&&(n=e),n}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var zn=class{isErrorState(n,e){let i=e&&e.submitted;return!!(n&&n.invalid&&(n.dirty||n.touched||i))}};var ok=()=>["producto","cantidad","precio","opciones"];function w3(t,n){t&1&&(f(0,"th",14),g(1,"Producto"),p()),t&2&&x("text-align","center")}function E3(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),Cr(" ",e.producto.sku," ",e.producto.nombre," ")}}function x3(t,n){t&1&&(f(0,"th",14),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function S3(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad)}}function M3(t,n){t&1&&(f(0,"th",14),g(1,"Precio"),p()),t&2&&x("text-align","center")}function I3(t,n){if(t&1&&(f(0,"td",15),g(1),Gt(2,"currency"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",mi(2,3,e.precio_unitario_venta))}}function T3(t,n){t&1&&(f(0,"th",16),g(1,"OPCIONES"),p())}function A3(t,n){if(t&1){let e=Ke();f(0,"td",15)(1,"button",17),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.aumentarDetallePedido(r))}),f(2,"mat-icon"),g(3,"arrow_circle_up"),p()(),f(4,"button",17),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.disminuirDetallePedido(r))}),f(5,"mat-icon"),g(6,"arrow_circle_down"),p()()()}}function R3(t,n){t&1&&z(0,"tr",18)}function k3(t,n){t&1&&z(0,"tr",19)}function O3(t,n){if(t&1&&z(0,"textarea",4),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function N3(t,n){if(t&1&&(f(0,"mat-option",8),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function P3(t,n){if(t&1){let e=Ke();f(0,"mat-select",7),F("selectionChange",function(r){Ae(e);let o=b(2);return Re(o.onSelectionChange(r))}),Qe(1,N3,2,2,"mat-option",8,Ye),p()}if(t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function F3(t,n){if(t&1&&z(0,"input",6),t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function L3(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function V3(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function j3(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function B3(t,n){if(t&1&&(f(0,"div")(1,"mat-form-field")(2,"mat-label"),g(3),p(),T(4,O3,1,2,"textarea",4)(5,P3,3,3,"mat-select",5)(6,F3,1,3,"input",6),T(7,L3,2,1,"mat-hint"),T(8,V3,2,1,"mat-error"),T(9,j3,2,1,"mat-error"),p()()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(3),me(a.label),h(),A((e=a.controlType)==="textarea"?4:e==="select"?5:6),h(3),A((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),h(),A((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),h(),A((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}function H3(t,n){if(t&1&&z(0,"textarea",4),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function U3(t,n){if(t&1&&(f(0,"mat-option",7),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function z3(t,n){if(t&1&&(f(0,"mat-select",5),Qe(1,U3,2,2,"mat-option",7,Ye),p()),t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function $3(t,n){if(t&1&&z(0,"input",6),t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function G3(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function W3(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function q3(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function Y3(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),T(4,H3,1,2,"textarea",4)(5,z3,3,3,"mat-select",5)(6,$3,1,3,"input",6),T(7,G3,2,1,"mat-hint"),T(8,W3,2,1,"mat-error"),T(9,q3,2,1,"mat-error"),p()()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(3),me(a.label),h(),A((e=a.controlType)==="textarea"?4:e==="select"?5:6),h(3),A((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),h(),A((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),h(),A((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}var Dh=class t{innerWidths="0";document=d(j);dialog=d(Zi);usuarioService=d(Un);carritoService=d(Ii);usuario=S(L.usuarioVacio());pedido=S(L.pedidoVacio());detallePedidoList=S([]);constructor(){}ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=ve.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},this.dialog.open(K0,n).afterClosed().subscribe(i=>{this.cargarListas()})}agregarDetallePedido(){let n=ve.getMatDialogConf();n.data={pedido:this.pedido()},this.dialog.open(X0,n).afterClosed().subscribe(i=>{this.cargarListas()})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-carrito"]],decls:27,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Carrito de compras "),p()(),f(5,"table",2),te(6,3),N(7,w3,2,2,"th",4)(8,E3,2,4,"td",5),ne(),te(9,6),N(10,x3,2,2,"th",4)(11,S3,2,3,"td",5),ne(),te(12,7),N(13,M3,2,2,"th",4)(14,I3,3,5,"td",5),ne(),te(15,8),N(16,T3,2,0,"th",9)(17,A3,7,0,"td",10),ne(),N(18,R3,1,0,"tr",11)(19,k3,1,0,"tr",12),p()(),f(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),F("click",function(){return i.pagoCarrito()}),g(24,"Pagar Carrito"),p(),f(25,"button",13),F("click",function(){return i.agregarDetallePedido()}),g(26,"Agregar Producto"),p()()()()()),e&2&&(h(),x("overflow","auto"),h(4),E("dataSource",i.detallePedidoList()),h(13),E("matHeaderRowDef",ot(5,ok)),h(),E("matRowDefColumns",ot(6,ok)))},dependencies:[Mi,Tr,ri,oi,Ei,Ir,ut,eo,Bn,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,wr],encapsulation:2})},K0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(j);_snackBar=d(qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);dataService=d(Je);carritoService=d(Ii);usuarioService=d(Un);pagoCarritoForm;detallePedidoList=S([]);pasarelaList=S([]);totalPedido=S(0);comisionPasarela=S(0);totalPago=S(0);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLineaFormControls(this.pasarelaList());this.pagoCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.cargarDatosForm()}getFormControl(n){return this.pagoCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionChange(n){let e=n.value;this.comisionPasarela.update(i=>e.comision),this.totalPago.update(i=>this.totalPedido()+e.comision)}pagarPedido(){if(this.validarDatos())if(this.totalPago()>0&&this.totalPedido()>0){var n=this.detallePedidoList()[0].pedido;this.carritoService.pedidoPagado(n),ve.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else ve.openSnackBar("Datos Incorrectos","aceptar",this._snackBar);else ve.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.pagoCarritoForm);return n}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:32,vars:10,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),g(5,"Pagar Pedido"),p()(),f(6,"mat-card-content")(7,"form",1),F("ngSubmit",function(){return i.pagarPedido()}),f(8,"mat-grid-list",2)(9,"div")(10,"mat-label"),g(11,"Total de pedido: "),p(),f(12,"mat-label"),g(13),Gt(14,"currency"),p()(),Qe(15,B3,10,5,"div",null,Ye),f(17,"div")(18,"mat-label"),g(19,"comision pasarela: "),p(),f(20,"mat-label"),g(21),Gt(22,"currency"),p()(),f(23,"div")(24,"mat-label"),g(25,"Total a pagar: "),p(),f(26,"mat-label"),g(27),Gt(28,"currency"),p()(),f(29,"div")(30,"button",3),g(31,"Pagar"),p()()()()()()()()),e&2&&(h(7),E("formGroup",i.pagoCarritoForm),h(6),me(mi(14,4,i.totalPedido())),h(2),Ze(i.formConfigs()),h(6),me(mi(22,6,i.comisionPasarela())),h(6),me(mi(28,8,i.totalPago())))},dependencies:[Si,ut,nn,Ji,tr,er,yn,rn,Vn,jn,nr,Ar,_n,xi,wi,Fn,Ln,Di,ii,Yt,ni,Jr,wr],encapsulation:2})},X0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px"}innerWidths="0";document=d(j);_snackBar=d(qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);carritoService=d(Ii);dataService=d(Je);usuarioService=d(Un);agregarCarritoForm;productoList=S([]);pedido=L.pedidoVacio();ngOnInit(){this.cargarDatos(),this.cargarListas();var n=this.formService.newAgregarDetallePedidoControls(this.productoList());this.agregarCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}getFormControl(n){return this.agregarCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(){var n=this.usuarioService.getUsuarioLoggeado();this.pedido=this.carritoService.getUltimoPedidoUsuario(n)}cargarListas(){this.productoList.update(n=>[...this.carritoService.getProductosStocWeb()])}agregarDetallePedido(){if(this.validarDatos()){let n=this.agregarCarritoForm.value,e=L.detallePedidoVacio();e.pedido=this.pedido,e.cantidad=n.cantidad,e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.dataService.pushDetallePedido(e),ve.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else ve.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCarritoForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(g(0,"3"),f(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),g(6,"Agregar Producto"),p()(),f(7,"mat-card-content")(8,"form",1),F("ngSubmit",function(){return i.agregarDetallePedido()}),f(9,"mat-grid-list",2),Qe(10,Y3,10,5,"mat-card-title",null,Ye),f(12,"mat-card-title")(13,"button",3),g(14,"Agregar"),p()()()()()()()()),e&2&&(h(8),E("formGroup",i.agregarCarritoForm),h(2),Ze(i.formConfigs()))},dependencies:[Si,ut,nn,Ji,tr,er,yn,rn,Vn,jn,nr,Ar,_n,xi,wi,Fn,Ln,Di,ii,Yt,ni,Jr],encapsulation:2})};var Il=class t{documento=d(j);localStorageService=d(on);theme=S("light");constructor(){Li(()=>{let n=this.theme;this.localStorageService.setItem("app-theme",n()?n():"light");let e=this.documento.documentElement;n()==="dark"?(e.classList.add("dark"),e.classList.remove("light")):(e.classList.add("light"),e.classList.remove("dark"))})}toggleTheme(){this.theme.update(n=>n==="light"?"dark":"light")}setTheme(n){this.theme.set(n)}getTheme(){return this.localStorageService.getItem("app-theme")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};function Q3(t,n){t&1&&(f(0,"mat-icon"),g(1,"account_circle"),p())}function Z3(t,n){t&1&&(f(0,"mat-icon"),g(1,"menu"),p())}function K3(t,n){if(t&1&&(f(0,"button",3),g(1),p()),t&2){let e=n.$implicit;E("routerLink",e.ruta),h(),me(e.nombre)}}function X3(t,n){if(t&1){let e=Ke();f(0,"button",9),F("click",function(){Ae(e);let r=b();return Re(r.cerrarSesion())}),f(1,"mat-icon"),g(2,"account_circle_off"),p(),f(3,"span"),g(4,"Cerrar Sesion"),p()(),z(5,"mat-divider")}if(t&2){let e=b();E("hidden",e.logged()==!1)}}var wh=class t{logged=S(!1);isChecked=S(!1);isLightTheme=S(!0);localStorage=d(on);themeService=d(Il);_router=d(gn);menusList=S([]);constructor(){}ngOnInit(){this.cargarMenusDeMantenimientos(),this.validarUsuarioLogeado();var n=this.themeService.getTheme();this.isLightTheme.update(e=>n=="light")}onThemeSwitchChange(){this.themeService.toggleTheme()}routerActivated(n){this.validarUsuarioLogeado()}cargarMenusDeMantenimientos(){var n=[];n.push({id:1,ruta:"/menu/login",nombre:"Iniciar Sesion"}),n.push({id:2,ruta:"/menu/productos",nombre:"Productos"}),n.push({id:3,ruta:"/menu/carrito",nombre:"Carrito"}),n.push({id:4,ruta:"/menu/ventas",nombre:"Ventas"}),n.push({id:5,ruta:"/menu/compras",nombre:"Compras"}),n.push({id:6,ruta:"/menu/inventario",nombre:"Inventario"}),this.menusList.update(e=>[...n])}homeClick(){let n={queryParams:{nada:"xd"}};this._router.navigate(["/menu/productos"],n)}cerrarSesion(){let n={queryParams:{logged:"false"}};this.localStorage.setItem(L.LOGGED,"false"),this._router.navigate(["/menu/login"],n)}limpiarDatos(){this.localStorage.clear()}validarUsuarioLogeado(){let n=this.localStorage.getItem(L.LOGGED);this.logged.update(e=>n==!0)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-inicio"]],decls:25,vars:14,consts:[["menu","matMenu"],["matIconButton","",3,"matMenuTriggerFor"],["src","logo5.jpg",3,"click"],["mat-menu-item","","routerLinkActive","active-menu-item",3,"routerLink"],["mat-menu-item","",3,"click"],[3,"ngModelChange","change","ngModel"],[1,"container-fluid"],[1,"row"],[3,"activate"],["mat-menu-item","",3,"click","hidden"]],template:function(e,i){if(e&1){let r=Ke();f(0,"mat-toolbar")(1,"button",1),T(2,Q3,2,0,"mat-icon"),T(3,Z3,2,0,"mat-icon"),p(),z(4,"span"),f(5,"span")(6,"img",2),F("click",function(){return i.homeClick()}),p()(),z(7,"span"),p(),f(8,"mat-menu",null,0),Qe(10,K3,2,2,"button",3,Ye),z(12,"mat-divider"),T(13,X3,6,1),f(14,"button",4),F("click",function(){return i.limpiarDatos()}),f(15,"mat-icon"),g(16,"clear"),p(),f(17,"span"),g(18,"Limpiar Datos"),p()(),z(19,"mat-divider"),f(20,"mat-slide-toggle",5),ym("ngModelChange",function(a){return Ae(r),ry(i.isChecked,a)||(i.isChecked=a),Re(a)}),F("change",function(){return i.onThemeSwitchChange()}),g(21),p()(),f(22,"div",6)(23,"div",7)(24,"router-outlet",8),F("activate",function(a){return i.routerActivated(a)}),p()()()}if(e&2){let r=zt(9);h(),E("matMenuTriggerFor",r),h(),A(i.logged()?2:-1),h(),A(i.logged()?-1:3),h(),x("margin-left","1em"),h(2),x("height","100%")("max-height","2em"),h(),x("margin-left","1em"),h(3),Ze(i.menusList()),h(3),A(i.logged()?13:-1),h(7),_m("ngModel",i.isChecked),h(),$("Colores: ",i.isChecked()?"Oscuro":"Claro")}},dependencies:[Ch,ik,Bn,xl,Wd,VR,N0,Ln,p0,Na,Jr,hA,rl,wb,Ho,eo],encapsulation:2})};function J3(t,n){if(t&1&&(f(0,"label",1),g(1),p()),t&2){let e=b();h(),me(e.mensajeLogin)}}function e4(t,n){t&1&&(f(0,"mat-error"),g(1,"Ingrese Usuario"),p())}function t4(t,n){t&1&&(f(0,"mat-error"),g(1,"Ingrese Contrase\xF1a"),p())}var Eh=class t{constructor(n,e){this._router=n;this.formBuilder=e;this.loginForm=this.formBuilder.group({username:["",[H.required,H.minLength(3)]],password:["",[H.required,H.minLength(3)]]})}loginForm;mensajeLogin="";_snackBar=d(qt);usuarioService=d(Un);localStorage=d(on);matcher=new zn;login(){if(this.validarDatos()){var n=this.usuarioService.validarUsuario(this.loginF("username")?.value,this.loginF("password")?.value);if(n&&n.id>1){ve.openSnackBar("Login exitoso","ok",this._snackBar),this.localStorage.setItem(L.LOGGED_USUARIO,n),this.localStorage.setItem(L.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}else ve.openSnackBar("Credenciales incorrectas","ok",this._snackBar)}else ve.openSnackBar("Completar credenciales","ok",this._snackBar)}iniciarInvitado(){var n=this.usuarioService.getUsuarioAnonimo();this.localStorage.setItem(L.LOGGED_USUARIO,n),this.localStorage.setItem(L.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}validarDatos(){var n=this.loginForm.valid;return this.loginF("username")?.valid||(n=!1),this.loginF("password")?.valid||(n=!1),n}loginF(n){return this.loginForm.get(n)}static \u0275fac=function(e){return new(e||t)(ee(gn),ee(th))};static \u0275cmp=O({type:t,selectors:[["app-login"]],decls:28,vars:6,consts:[["hideToggle","","expanded","true",1,"login-container"],[1,"h4"],["id","loginForm",3,"ngSubmit","formGroup"],["cols","1"],["matInput","","required","","formControlName","username",3,"errorStateMatcher"],["matInput","","required","","formControlName","password","type","password",3,"errorStateMatcher"],["mat-stroked-button","","color","primary","type","submit"],["matButton","",3,"click"]],template:function(e,i){if(e&1&&(f(0,"mat-expansion-panel",0)(1,"mat-expansion-panel-header")(2,"mat-panel-title"),g(3," Iniciar Sesi\xF3n "),p()(),f(4,"mat-card")(5,"mat-card-title"),T(6,J3,2,1,"label",1),p(),f(7,"mat-card-content")(8,"form",2),F("ngSubmit",function(){return i.login()}),f(9,"mat-grid-list",3)(10,"mat-card-title")(11,"mat-form-field")(12,"mat-label"),g(13,"Usuario"),p(),z(14,"input",4),T(15,e4,2,0,"mat-error"),p()(),f(16,"mat-card-title")(17,"mat-form-field")(18,"mat-label"),g(19,"Contrase\xF1a"),p(),z(20,"input",5),T(21,t4,2,0,"mat-error"),p()(),f(22,"mat-card-title")(23,"button",6),g(24,"Ingresar"),p()()()()()(),f(25,"mat-action-row")(26,"button",7),F("click",function(){return i.iniciarInvitado()}),g(27,"Continuar como invitado"),p()()()),e&2){let r,o;h(6),A(i.mensajeLogin&&i.mensajeLogin!=""?6:-1),h(2),E("formGroup",i.loginForm),h(6),E("errorStateMatcher",i.matcher),h(),A((r=i.loginF("username"))!=null&&r.errors?15:-1),h(5),E("errorStateMatcher",i.matcher),h(),A((o=i.loginF("password"))!=null&&o.errors?21:-1)}},dependencies:[bh,ri,SR,oi,Ei,ut,Ji,tr,er,yn,rn,jn,nr,xi,wi,Fn,Ln,Di,ii,Yt,ni],encapsulation:2})};var ak=()=>["sku","nombre","descripcion","precio_venta","estado","opciones"];function n4(t,n){if(t&1){let e=Ke();f(0,"button",16),F("click",function(){Ae(e);let r=b();return Re(r.crearProducto())}),g(1,"Crear Producto"),p()}}function i4(t,n){if(t&1){let e=Ke();f(0,"button",16),F("click",function(){Ae(e);let r=b();return Re(r.crearProducto())}),g(1,"Editar Producto"),p()}}function r4(t,n){t&1&&(f(0,"th",17),g(1,"Codigo"),p()),t&2&&x("text-align","center")}function o4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.sku," ")}}function a4(t,n){t&1&&(f(0,"th",17),g(1,"Nombre"),p()),t&2&&x("text-align","center")}function s4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.nombre)}}function l4(t,n){t&1&&(f(0,"th",17),g(1,"Descripcion"),p()),t&2&&x("text-align","center")}function c4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.descripcion)}}function d4(t,n){t&1&&(f(0,"th",17),g(1,"Precio de Venta"),p()),t&2&&x("text-align","center")}function u4(t,n){if(t&1&&(f(0,"td",18),g(1),Gt(2,"currency"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",mi(2,3,e.precio_venta)," ")}}function f4(t,n){t&1&&(f(0,"th",17),g(1,"Periodo"),p()),t&2&&x("text-align","center")}function m4(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.estado.descripcion," ")}}function p4(t,n){t&1&&(f(0,"th",19),g(1,"Editar"),p())}function h4(t,n){if(t&1){let e=Ke();f(0,"td",18)(1,"button",20),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.editarProducto(r))}),f(2,"mat-icon"),g(3,"edit"),p()()()}}function g4(t,n){t&1&&z(0,"tr",21)}function v4(t,n){if(t&1){let e=Ke();f(0,"tr",22),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.seleccionarProducto(r))}),p()}}function _4(t,n){t&1&&(f(0,"label",0),g(1,"Agregar Producto"),p())}function y4(t,n){t&1&&(f(0,"label",0),g(1,"Editar Producto"),p())}function b4(t,n){if(t&1&&z(0,"textarea",4),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function C4(t,n){if(t&1&&(f(0,"mat-option",7),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function D4(t,n){if(t&1&&(f(0,"mat-select",5),Qe(1,C4,2,2,"mat-option",7,Ye),p()),t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function w4(t,n){if(t&1&&z(0,"input",6),t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function E4(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function x4(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function S4(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function M4(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),T(4,b4,1,2,"textarea",4)(5,D4,3,3,"mat-select",5)(6,w4,1,3,"input",6),T(7,E4,2,1,"mat-hint"),T(8,x4,2,1,"mat-error"),T(9,S4,2,1,"mat-error"),p()()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(3),me(a.label),h(),A((e=a.controlType)==="textarea"?4:e==="select"?5:6),h(3),A((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),h(),A((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),h(),A((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}function I4(t,n){t&1&&(f(0,"button",3),g(1,"Agregar"),p())}function T4(t,n){t&1&&(f(0,"button",3),g(1,"Editar"),p())}var xh=class t{innerWidths="0";document=d(j);dialog=d(Zi);dataService=d(Je);productoSeleccionado=L.productoVacio();productosList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.productosList.update(()=>[...this.dataService.getProductos()])}seleccionarProducto(n){this.productoSeleccionado=n}crearProducto(){let n=ve.getMatDialogConf();n.data={productoSeleccionado:this.productoSeleccionado},this.dialog.open(J0,n).afterClosed().subscribe(i=>{this.cargarListas(),this.productoSeleccionado=L.productoVacio()})}editarProducto(n){this.productoSeleccionado=n,this.crearProducto()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-productos"]],decls:31,vars:9,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-stroked-button","","color","primary"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","nombre"],["matColumnDef","descripcion"],["matColumnDef","precio_venta"],["matColumnDef","estado"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",3,"click",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matMiniFab","",3,"click"],["mat-header-row",""],["mat-row","",3,"click"]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-description"),T(4,n4,2,0,"button",2),T(5,i4,2,0,"button",2),p()()(),f(6,"mat-expansion-panel",1)(7,"mat-expansion-panel-header")(8,"mat-panel-title"),g(9," Tabla Productos "),p()(),f(10,"table",3),te(11,4),N(12,r4,2,2,"th",5)(13,o4,2,3,"td",6),ne(),te(14,7),N(15,a4,2,2,"th",5)(16,s4,2,3,"td",6),ne(),te(17,8),N(18,l4,2,2,"th",5)(19,c4,2,3,"td",6),ne(),te(20,9),N(21,d4,2,2,"th",5)(22,u4,3,5,"td",6),ne(),te(23,10),N(24,f4,2,2,"th",5)(25,m4,2,3,"td",6),ne(),te(26,11),N(27,p4,2,0,"th",12)(28,h4,4,0,"td",13),ne(),N(29,g4,1,0,"tr",14)(30,v4,1,0,"tr",15),p()()()),e&2&&(h(4),A(!i.productoSeleccionado||i.productoSeleccionado.id==0?4:-1),h(),A(!i.productoSeleccionado||i.productoSeleccionado.id>0?5:-1),h(),x("overflow","auto"),h(4),E("dataSource",i.productosList()),h(19),E("matHeaderRowDef",ot(7,ak)),h(),E("matRowDefColumns",ot(8,ak)))},dependencies:[Mi,Tr,ri,oi,Ei,Ir,ut,Hp,Bn,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,wr],encapsulation:2})},J0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.seleccionarProducto(n.productoSeleccionado)}innerWidths="0";document=d(j);_snackBar=d(qt);dataService=d(Je);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);productoForm;productoSeleccionado=L.productoVacio();estadoProductoList=S([]);ngOnInit(){this.cargarListas();var n=this.formService.newProductoControls(this.estadoProductoList());this.productoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.seleccionarProductoForm()}getFormControl(n){return this.productoForm.get(n)}compareIds(n,e){return n.id==e.id}seleccionarProducto(n){this.productoSeleccionado=n}seleccionarProductoForm(){this.getFormControl("id")?.setValue(this.productoSeleccionado.id),this.getFormControl("sku")?.setValue(this.productoSeleccionado.sku),this.getFormControl("nombre")?.setValue(this.productoSeleccionado.nombre),this.getFormControl("descripcion")?.setValue(this.productoSeleccionado.descripcion),this.getFormControl("precio_venta")?.setValue(this.productoSeleccionado.precio_venta),this.getFormControl("estado")?.setValue(this.productoSeleccionado.estado)}cargarListas(){this.estadoProductoList.update(n=>[...this.dataService.getEstadosProducto()])}guardarProducto(){if(this.validarDatos()){if(this.productoSeleccionado.id>0){var n=this.productoSeleccionado.id;this.productoSeleccionado=this.productoForm.value,this.productoSeleccionado.id=n,this.dataService.editarProducto(this.productoSeleccionado),ve.openSnackBar("CAMBIOS GUARDADOS EXITOSAMENTE","aceptar",this._snackBar)}else this.productoSeleccionado=this.productoForm.value,this.dataService.pushProducto(this.productoSeleccionado),ve.openSnackBar("NUEVO PRODUCTO CREADO EXITOSAMENTE","aceptar",this._snackBar);this.dialogRef.close()}else ve.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.productoForm),e=parseFloat(this.getFormControl("precio_venta")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("precio_venta")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:16,vars:5,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),g(5,"Formulario"),p(),T(6,_4,2,0,"label",0),T(7,y4,2,0,"label",0),p(),f(8,"mat-card-content")(9,"form",1),F("ngSubmit",function(){return i.guardarProducto()}),f(10,"mat-grid-list",2),Qe(11,M4,10,5,"mat-card-title",null,Ye),f(13,"mat-card-title"),T(14,I4,2,0,"button",3),T(15,T4,2,0,"button",3),p()()()()()()()),e&2&&(h(6),A(!i.productoSeleccionado||i.productoSeleccionado.id==0?6:-1),h(),A(i.productoSeleccionado&&i.productoSeleccionado.id>0?7:-1),h(2),E("formGroup",i.productoForm),h(2),Ze(i.formConfigs()),h(3),A(!i.productoSeleccionado||i.productoSeleccionado.id==0?14:-1),h(),A(i.productoSeleccionado&&i.productoSeleccionado.id>0?15:-1))},dependencies:[Si,ut,nn,Ji,tr,er,yn,rn,Vn,jn,nr,Ar,_n,xi,wi,Fn,Ln,Di,ii,Yt,ni],encapsulation:2})};var sk=()=>["producto","cantidad","precio","opciones"];function A4(t,n){t&1&&(f(0,"th",14),g(1,"Producto"),p()),t&2&&x("text-align","center")}function R4(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),Cr(" ",e.producto.sku," ",e.producto.nombre," ")}}function k4(t,n){t&1&&(f(0,"th",14),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function O4(t,n){if(t&1&&(f(0,"td",15),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad)}}function N4(t,n){t&1&&(f(0,"th",14),g(1,"Precio"),p()),t&2&&x("text-align","center")}function P4(t,n){if(t&1&&(f(0,"td",15),g(1),Gt(2,"currency"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",mi(2,3,e.precio_unitario_venta))}}function F4(t,n){t&1&&(f(0,"th",16),g(1,"OPCIONES"),p())}function L4(t,n){if(t&1){let e=Ke();f(0,"td",15)(1,"button",17),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.aumentarDetallePedido(r))}),f(2,"mat-icon"),g(3,"arrow_circle_up"),p()(),f(4,"button",17),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.disminuirDetallePedido(r))}),f(5,"mat-icon"),g(6,"arrow_circle_down"),p()()()}}function V4(t,n){t&1&&z(0,"tr",18)}function j4(t,n){t&1&&z(0,"tr",19)}function B4(t,n){t&1&&g(0,"Datos Cliente")}function H4(t,n){if(t&1&&z(0,"textarea",8),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function U4(t,n){if(t&1&&(f(0,"mat-option",11),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function z4(t,n){if(t&1&&(f(0,"mat-select",9),Qe(1,U4,2,2,"mat-option",11,Ye),p()),t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function $4(t,n){if(t&1&&z(0,"input",10),t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function G4(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function W4(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function q4(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function Y4(t,n){if(t&1&&(f(0,"mat-form-field")(1,"mat-label"),g(2),p(),T(3,H4,1,2,"textarea",8)(4,z4,3,3,"mat-select",9)(5,$4,1,3,"input",10),T(6,G4,2,1,"mat-hint"),T(7,W4,2,1,"mat-error"),T(8,q4,2,1,"mat-error"),p()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(2),me(a.label),h(),A((e=a.controlType)==="textarea"?3:e==="select"?4:5),h(3),A((i=s.getFormControlCliente(a.name))!=null&&i.errors?6:-1),h(),A((r=s.getFormControlCliente(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),h(),A((o=s.getFormControlCliente(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function Q4(t,n){t&1&&g(0,"Tipo Pago")}function Z4(t,n){if(t&1&&z(0,"textarea",8),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function K4(t,n){if(t&1&&(f(0,"mat-option",11),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function X4(t,n){if(t&1){let e=Ke();f(0,"mat-select",12),F("selectionChange",function(r){Ae(e);let o=b(2);return Re(o.onSelectionTipoPagoChange(r))}),Qe(1,K4,2,2,"mat-option",11,Ye),p()}if(t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function J4(t,n){if(t&1&&z(0,"input",10),t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function e5(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function t5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function n5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function i5(t,n){if(t&1&&(f(0,"mat-form-field")(1,"mat-label"),g(2),p(),T(3,Z4,1,2,"textarea",8)(4,X4,3,3,"mat-select",9)(5,J4,1,3,"input",10),T(6,e5,2,1,"mat-hint"),T(7,t5,2,1,"mat-error"),T(8,n5,2,1,"mat-error"),p()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(2),me(a.label),h(),A((e=a.controlType)==="textarea"?3:e==="select"?4:5),h(3),A((i=s.getFormControlTipoPago(a.name))!=null&&i.errors?6:-1),h(),A((r=s.getFormControlTipoPago(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),h(),A((o=s.getFormControlTipoPago(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function r5(t,n){t&1&&g(0,"Tipo Pago")}function o5(t,n){if(t&1&&z(0,"textarea",8),t&2){let e=b().$implicit,i=b(2);E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function a5(t,n){if(t&1&&(f(0,"mat-option",11),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function s5(t,n){if(t&1&&(f(0,"mat-select",9),Qe(1,a5,2,2,"mat-option",11,Ye),p()),t&2){let e=b().$implicit,i=b(2);E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function l5(t,n){if(t&1&&z(0,"input",10),t&2){let e=b().$implicit,i=b(2);E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function c5(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function d5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function u5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function f5(t,n){if(t&1&&(f(0,"mat-form-field")(1,"mat-label"),g(2),p(),T(3,o5,1,2,"textarea",8)(4,s5,3,3,"mat-select",9)(5,l5,1,3,"input",10),T(6,c5,2,1,"mat-hint"),T(7,d5,2,1,"mat-error"),T(8,u5,2,1,"mat-error"),p()),t&2){let e,i,r,o,a=n.$implicit,s=b(2);h(2),me(a.label),h(),A((e=a.controlType)==="textarea"?3:e==="select"?4:5),h(3),A((i=s.getFormControlTipoPago(a.name))!=null&&i.errors?6:-1),h(),A((r=s.getFormControlTipoPago(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),h(),A((o=s.getFormControlTipoPago(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function m5(t,n){if(t&1&&(f(0,"mat-step",2)(1,"form",3),N(2,r5,1,0,"ng-template",4),Qe(3,f5,9,5,"mat-form-field",null,Ye),f(5,"div")(6,"button",6),g(7,"Regresar"),p(),f(8,"button",5),g(9,"Siguiente"),p()()()()),t&2){let e=b();E("stepControl",e.datosTarjetaForm),h(),E("formGroup",e.datosTarjetaForm),h(2),Ze(e.formConfigsTarjeta())}}function p5(t,n){t&1&&g(0,"Done")}var nC=".fade-in[_ngcontent-%COMP%]{opacity:0;transform:translateY(10px);transition:opacity .4s ease,transform .4s ease}div[_ngcontent-%COMP%]{opacity:1;transform:translateY(0)}.fade-out[_ngcontent-%COMP%]{opacity:1;transform:translateY(0);transition:opacity .4s ease,transform .4s ease}.fade-out.leave-active[_ngcontent-%COMP%]{opacity:0;transform:translateY(-10px)}";function h5(t,n){if(t&1&&z(0,"textarea",4),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function g5(t,n){if(t&1&&(f(0,"mat-option",7),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function v5(t,n){if(t&1&&(f(0,"mat-select",5),Qe(1,g5,2,2,"mat-option",7,Ye),p()),t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function _5(t,n){if(t&1&&z(0,"input",6),t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function y5(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function b5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function C5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function D5(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),T(4,h5,1,2,"textarea",4)(5,v5,3,3,"mat-select",5)(6,_5,1,3,"input",6),T(7,y5,2,1,"mat-hint"),T(8,b5,2,1,"mat-error"),T(9,C5,2,1,"mat-error"),p()()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(3),me(a.label),h(),A((e=a.controlType)==="textarea"?4:e==="select"?5:6),h(3),A((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),h(),A((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),h(),A((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}var Sh=class t{constructor(n){this._router=n}_snackBar=d(qt);innerWidths="0";document=d(j);dialog=d(Zi);usuarioService=d(Un);carritoService=d(Ii);usuario=S(L.usuarioVacio());pedido=S(L.pedidoVacio());detallePedidoList=S([]);ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=ve.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},this.dialog.open(eC,n).afterClosed().subscribe(i=>{this.cargarListas()})}agregarDetallePedido(){if(this.usuario()&&this.usuario().id>0){let n=ve.getMatDialogConf();n.data={pedido:this.pedido()},this.dialog.open(tC,n).afterClosed().subscribe(i=>{this.cargarListas()})}else{ve.openSnackBar("Usuario no registrado","aceptar",this._snackBar);let n={queryParams:{logged:"true"}};this._router.navigate(["/menu/login"],n)}}static \u0275fac=function(e){return new(e||t)(ee(gn))};static \u0275cmp=O({type:t,selectors:[["app-ventas"]],decls:27,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Ventas "),p()(),f(5,"table",2),te(6,3),N(7,A4,2,2,"th",4)(8,R4,2,4,"td",5),ne(),te(9,6),N(10,k4,2,2,"th",4)(11,O4,2,3,"td",5),ne(),te(12,7),N(13,N4,2,2,"th",4)(14,P4,3,5,"td",5),ne(),te(15,8),N(16,F4,2,0,"th",9)(17,L4,7,0,"td",10),ne(),N(18,V4,1,0,"tr",11)(19,j4,1,0,"tr",12),p()(),f(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),F("click",function(){return i.pagoCarrito()}),g(24,"Registrar Venta"),p(),f(25,"button",13),F("click",function(){return i.agregarDetallePedido()}),g(26,"Agregar Producto"),p()()()()()),e&2&&(h(),x("overflow","auto"),h(4),E("dataSource",i.detallePedidoList()),h(13),E("matHeaderRowDef",ot(5,sk)),h(),E("matRowDefColumns",ot(6,sk)))},dependencies:[Mi,Tr,ri,oi,Ei,Ir,ut,eo,Bn,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,wr],styles:[nC]})},eC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(j);_snackBar=d(qt);dialogRef=d(Vt);formConfigsCliente=S([]);formConfigsTipoPago=S([]);formConfigsTarjeta=S([]);matcher=new zn;formService=d(Hn);dataService=d(Je);carritoService=d(Ii);usuarioService=d(Un);datosClienteForm;datosTarjetaForm;tipoPagoForm;detallePedidoList=S([]);pasarelaList=S([]);totalPedido=S(0);comisionPasarela=S(0);totalPago=S(0);esPagoTarjeta=S(!1);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLocalClienteFormControls(),e=this.formService.newVentaLocalTipoPagoFormControls(),i=this.formService.newVentaLocalFormControls(this.pasarelaList());this.datosClienteForm=this.formService.getFormGroup(n),this.tipoPagoForm=this.formService.getFormGroup(e),this.datosTarjetaForm=this.formService.getFormGroup(i),this.formConfigsCliente.update(r=>[...n]),this.formConfigsTipoPago.update(r=>[...e]),this.formConfigsTarjeta.update(r=>[...i]),this.cargarDatosForm()}getFormControlCliente(n){return this.datosClienteForm.get(n)}getFormControlTipoPago(n){return this.tipoPagoForm.get(n)}getFormControlTarjeta(n){return this.datosTarjetaForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionTipoPagoChange(n){let e=n.value;console.log("selected val"),console.log(e),e==2&&this.esPagoTarjeta.update(i=>!0),e==1&&this.esPagoTarjeta.update(i=>!1)}pagarPedido(){let n=!1;if(this.validarDatosCliente())if(this.validarDatosFormaPago())if(this.esPagoTarjeta()?this.validarDatosTarjeta()?n=!0:ve.openSnackBar("Datos de tarjeta incorrectos","aceptar",this._snackBar):n=!0,n&&this.totalPago()>0&&this.totalPedido()>0){var e=this.detallePedidoList()[0].pedido;this.carritoService.ventaPagada(e),ve.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else ve.openSnackBar("Datos de cliente incorrectos","aceptar",this._snackBar);else ve.openSnackBar("Datos de forma de pago incorrectos","aceptar",this._snackBar);else ve.openSnackBar("Datos de cliente incorrectos","ok",this._snackBar)}validarDatosCliente(){var n=this.formService.validateFormControls(this.datosClienteForm);return n}validarDatosFormaPago(){var n=this.formService.validateFormControls(this.tipoPagoForm);return n}validarDatosTarjeta(){var n=this.formService.validateFormControls(this.datosTarjetaForm);return n}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:34,vars:5,consts:[["stepper",""],["orientation","vertical","linear","true"],[3,"stepControl"],[3,"formGroup"],["matStepLabel",""],["matButton","","matStepperNext",""],["matButton","","matStepperPrevious",""],["matButton","",3,"click"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"]],template:function(e,i){if(e&1){let r=Ke();f(0,"mat-dialog-content")(1,"mat-stepper",1,0)(3,"mat-step",2)(4,"form",3),N(5,B4,1,0,"ng-template",4),Qe(6,Y4,9,5,"mat-form-field",null,Ye),f(8,"div")(9,"button",5),g(10,"Siguiente"),p()()()(),f(11,"mat-step",2)(12,"form",3),N(13,Q4,1,0,"ng-template",4),Qe(14,i5,9,5,"mat-form-field",null,Ye),f(16,"div")(17,"button",6),g(18,"Regresar"),p(),f(19,"button",5),g(20,"Siguiente"),p()()()(),T(21,m5,10,2,"mat-step",2),f(22,"mat-step"),N(23,p5,1,0,"ng-template",4),f(24,"p"),g(25,"Pagar Pedido"),p(),f(26,"div")(27,"button",7),F("click",function(){return i.pagarPedido()}),g(28,"Pagar"),p()(),f(29,"div")(30,"button",6),g(31,"Regresar"),p(),f(32,"button",7),F("click",function(){Ae(r);let a=zt(2);return Re(a.reset())}),g(33,"Reiniciar"),p()()()()()}e&2&&(h(3),E("stepControl",i.datosClienteForm),h(),E("formGroup",i.datosClienteForm),h(2),Ze(i.formConfigsCliente()),h(5),E("stepControl",i.tipoPagoForm),h(),E("formGroup",i.tipoPagoForm),h(2),Ze(i.formConfigsTipoPago()),h(7),A(i.esPagoTarjeta()?21:-1))},dependencies:[Si,ut,nn,yn,rn,Vn,jn,Ar,_n,xi,wi,Fn,Ln,Di,ii,Yt,ni,nk,Y0,Qd,Q0,ek,tk],styles:[nC]})},tC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px"}innerWidths="0";document=d(j);_snackBar=d(qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);carritoService=d(Ii);dataService=d(Je);usuarioService=d(Un);agregarCarritoForm;productoList=S([]);pedido=L.pedidoVacio();ngOnInit(){this.cargarDatos(),this.cargarListas();var n=this.formService.newAgregarDetallePedidoControls(this.productoList());this.agregarCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}getFormControl(n){return this.agregarCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(){var n=this.usuarioService.getUsuarioLoggeado();this.pedido=this.carritoService.getUltimoPedidoUsuario(n)}cargarListas(){this.productoList.update(n=>[...this.carritoService.getProductosStocLocal()])}agregarDetallePedido(){if(this.validarDatos()){let n=this.agregarCarritoForm.value,e=L.detallePedidoVacio();e.pedido=this.pedido,e.cantidad=n.cantidad,e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.dataService.pushDetallePedido(e),ve.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else ve.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCarritoForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(g(0,"3"),f(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),g(6,"Agregar Producto"),p()(),f(7,"mat-card-content")(8,"form",1),F("ngSubmit",function(){return i.agregarDetallePedido()}),f(9,"mat-grid-list",2),Qe(10,D5,10,5,"mat-card-title",null,Ye),f(12,"mat-card-title")(13,"button",3),g(14,"Agregar"),p()()()()()()()()),e&2&&(h(8),E("formGroup",i.agregarCarritoForm),h(2),Ze(i.formConfigs()))},dependencies:[Si,ut,nn,Ji,tr,er,yn,rn,Vn,jn,nr,Ar,_n,xi,wi,Fn,Ln,Di,ii,Yt,ni,Jr],styles:[nC]})};var Kd=class t{localStorageService=d(on);dataService=d(Je);constructor(){}getdetalleCompras(){var n=[],e=this.localStorageService.getItem(L.DETALLE_PEDIDOS_COMPRA_ID);return e&&(n=e),n}eliminarDetallesPedidoCompra(){this.localStorageService.removeItem(L.DETALLE_PEDIDOS_COMPRA_ID)}registrarCompra(){var n=this.getdetalleCompras();for(let e of n)this.registrarCompraInventario(e);this.eliminarDetallesPedidoCompra()}registrarCompraInventario(n){let e=L.loteInventarioVacio();e.cantidad_actual=n.cantidad,e.cantidad_inicial=n.cantidad,e.costo_unitario=n.precio_unitario_venta,e.fecha_ingreso=new Date,e.producto=n.producto,e=this.dataService.pushLoteInventario(e);let i=this.dataService.getTiposMovimiento(),r=L.movimientoInventarioVacio();r.cantidad=n.cantidad,r.fecha=new Date,r.producto=n.producto,r.tipo_movimiento=i[i.findIndex(o=>o.id==1)],r.lote=e,this.dataService.pushMovimientoInventario(r),n.producto.stock_web&&n.producto.stock_web>0?n.producto.stock_web+=n.cantidad_web:n.producto.stock_web=n.cantidad_web,n.producto.stock_local&&n.producto.stock_local>0?n.producto.stock_local+=n.cantidad_local:n.producto.stock_local=n.cantidad_local,this.dataService.editarProducto(n.producto)}aumentarDetalleCompra(n){var e=this.getdetalleCompras();for(let i of e)i.id==n.id&&(i.cantidad++,i.cantidad_web++,this.editarDetalleCompra(i));return e}disminuirDetalleCompra(n){var e=this.getdetalleCompras();for(let i of e)i.id==n.id&&i.cantidad>0&&(i.cantidad--,i.cantidad_web--,this.editarDetalleCompra(i));return e}editarDetalleCompra(n){var e=this.getdetalleCompras(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.DETALLE_PEDIDOS_COMPRA_ID,i)}pushDetalleCompra(n){var e=this.getdetalleCompras();e.length>0?n.id=e[e.length-1].id+1:n.id=1,e.push(n),this.localStorageService.setItem(L.DETALLE_PEDIDOS_COMPRA_ID,e)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var lk=()=>["producto","cantidad","cantidad_web","cantidad_local","precio","opciones"];function w5(t,n){t&1&&(f(0,"th",16),g(1,"Producto"),p()),t&2&&x("text-align","center")}function E5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),Cr(" ",e.producto.sku," ",e.producto.nombre," ")}}function x5(t,n){t&1&&(f(0,"th",16),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function S5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad)}}function M5(t,n){t&1&&(f(0,"th",16),g(1,"Cantidad Stock Web"),p()),t&2&&x("text-align","center")}function I5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad_web)}}function T5(t,n){t&1&&(f(0,"th",16),g(1,"Cantidad Stock Local"),p()),t&2&&x("text-align","center")}function A5(t,n){if(t&1&&(f(0,"td",17),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad_local)}}function R5(t,n){t&1&&(f(0,"th",16),g(1,"Precio"),p()),t&2&&x("text-align","center")}function k5(t,n){if(t&1&&(f(0,"td",17),g(1),Gt(2,"currency"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",mi(2,3,e.precio_unitario_venta))}}function O5(t,n){t&1&&(f(0,"th",18),g(1,"OPCIONES"),p())}function N5(t,n){if(t&1){let e=Ke();f(0,"td",17)(1,"button",19),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.aumentarDetalleCompra(r))}),f(2,"mat-icon"),g(3,"arrow_circle_up"),p()(),f(4,"button",19),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.disminuirDetalleCompra(r))}),f(5,"mat-icon"),g(6,"arrow_circle_down"),p()()()}}function P5(t,n){t&1&&z(0,"tr",20)}function F5(t,n){t&1&&z(0,"tr",21)}function L5(t,n){if(t&1&&z(0,"textarea",4),t&2){let e=b().$implicit,i=b();E("formControlName",e.name)("errorStateMatcher",i.matcher)}}function V5(t,n){if(t&1&&(f(0,"mat-option",7),g(1),p()),t&2){let e=n.$implicit;E("value",e.value),h(),me(e.name)}}function j5(t,n){if(t&1&&(f(0,"mat-select",5),Qe(1,V5,2,2,"mat-option",7,Ye),p()),t&2){let e=b().$implicit,i=b();E("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),h(),Ze(e.options)}}function B5(t,n){if(t&1){let e=Ke();f(0,"input",8),F("change",function(r){Ae(e);let o=b(2);return Re(o.inputChange(r))}),p()}if(t&2){let e=b().$implicit,i=b();E("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function H5(t,n){if(t&1&&(f(0,"mat-hint"),g(1),p()),t&2){let e=b().$implicit;h(),me(e.label)}}function U5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Seleccione ",e.label)}}function z5(t,n){if(t&1&&(f(0,"mat-error"),g(1),p()),t&2){let e=b().$implicit;h(),$("Ingrese ",e.label)}}function $5(t,n){if(t&1&&(f(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),g(3),p(),T(4,L5,1,2,"textarea",4)(5,j5,3,3,"mat-select",5)(6,B5,1,3,"input",6),T(7,H5,2,1,"mat-hint"),T(8,U5,2,1,"mat-error"),T(9,z5,2,1,"mat-error"),p()()),t&2){let e,i,r,o,a=n.$implicit,s=b();h(3),me(a.label),h(),A((e=a.controlType)==="textarea"?4:e==="select"?5:6),h(3),A((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),h(),A((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),h(),A((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}var Mh=class t{innerWidths="0";document=d(j);dialog=d(Zi);compraService=d(Kd);detalleCompraList=S([]);constructor(){}ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarListas()}cargarListas(){this.detalleCompraList.update(n=>[...this.compraService.getdetalleCompras()])}registrarCompra(){this.compraService.registrarCompra(),this.cargarListas()}aumentarDetalleCompra(n){this.detalleCompraList.update(e=>[...this.compraService.aumentarDetalleCompra(n)])}disminuirDetalleCompra(n){this.detalleCompraList.update(e=>[...this.compraService.disminuirDetalleCompra(n)])}agregarDetalleCompra(){let n=ve.getMatDialogConf();this.dialog.open(iC,n).afterClosed().subscribe(i=>{this.cargarListas()})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-compras"]],decls:33,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","cantidad_web"],["matColumnDef","cantidad_local"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Compra de productos "),p()(),f(5,"table",2),te(6,3),N(7,w5,2,2,"th",4)(8,E5,2,4,"td",5),ne(),te(9,6),N(10,x5,2,2,"th",4)(11,S5,2,3,"td",5),ne(),te(12,7),N(13,M5,2,2,"th",4)(14,I5,2,3,"td",5),ne(),te(15,8),N(16,T5,2,2,"th",4)(17,A5,2,3,"td",5),ne(),te(18,9),N(19,R5,2,2,"th",4)(20,k5,3,5,"td",5),ne(),te(21,10),N(22,O5,2,0,"th",11)(23,N5,7,0,"td",12),ne(),N(24,P5,1,0,"tr",13)(25,F5,1,0,"tr",14),p()(),f(26,"mat-expansion-panel",1)(27,"mat-expansion-panel-header")(28,"mat-panel-description")(29,"button",15),F("click",function(){return i.registrarCompra()}),g(30,"Registrar Compra"),p(),f(31,"button",15),F("click",function(){return i.agregarDetalleCompra()}),g(32,"Agregar Producto"),p()()()()()),e&2&&(h(),x("overflow","auto"),h(4),E("dataSource",i.detalleCompraList()),h(19),E("matHeaderRowDef",ot(5,lk)),h(),E("matRowDefColumns",ot(6,lk)))},dependencies:[Mi,Tr,ri,oi,Ei,Ir,ut,eo,Bn,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,wr],encapsulation:2})},iC=class t{innerWidths="0";document=d(j);_snackBar=d(qt);dialogRef=d(Vt);formConfigs=S([]);matcher=new zn;formService=d(Hn);dataService=d(Je);compraService=d(Kd);agregarCompraForm;productoList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas();var n=this.formService.newAgregarDetalleCompraControls(this.productoList());this.agregarCompraForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}inputChange(n){let e=this.getFormControl("cantidad"),i=this.getFormControl("cantidad_web"),r=this.getFormControl("cantidad_local"),o=e?.value|0,a=i?.value|0,s=r?.value|0;if(o>0){let l=0,c=o,u=0,m=o;a>0&&(m=o-a),s>0&&(c=o-s),o-(a+s)>0&&(l=o-(a+s),u=l),i?.setValidators([H.required,H.min(l),H.max(c)]),r?.setValidators([H.required,H.min(u),H.max(m)])}}getFormControl(n){return this.agregarCompraForm.get(n)}compareIds(n,e){return n.id==e.id}cargarListas(){this.productoList.update(n=>[...this.dataService.getProductos()])}agregarDetalleCompra(){if(this.validarDatos()){let n=this.agregarCompraForm.value;if(parseInt(n.cantidad)-(parseInt(n.cantidad_local)+parseInt(n.cantidad_web))>0){ve.openSnackBar("Cantidades no cuadran","ok",this._snackBar);return}let e=L.detalleCompraVacio();e.cantidad=parseInt(n.cantidad),e.cantidad_local=parseInt(n.cantidad_local),e.cantidad_web=parseInt(n.cantidad_web),e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.compraService.pushDetalleCompra(e),ve.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else ve.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCompraForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"],["matInput","","required","",3,"change","type","formControlName","errorStateMatcher"]],template:function(e,i){e&1&&(g(0,"3"),f(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),g(6,"Agregar Producto"),p()(),f(7,"mat-card-content")(8,"form",1),F("ngSubmit",function(){return i.agregarDetalleCompra()}),f(9,"mat-grid-list",2),Qe(10,$5,10,5,"mat-card-title",null,Ye),f(12,"mat-card-title")(13,"button",3),g(14,"Agregar"),p()()()()()()()()),e&2&&(h(8),E("formGroup",i.agregarCompraForm),h(2),Ze(i.formConfigs()))},dependencies:[Si,ut,nn,Ji,tr,er,yn,rn,Vn,jn,nr,Ar,_n,xi,wi,Fn,Ln,Di,ii,Yt,ni,Jr],encapsulation:2})};var Xd=class t{dataService=d(Je);_snackBar=d(qt);constructor(){}getMovimientosProducto(n){let e=this.dataService.getMovimientosInventario(),i=[];for(let r of e)r.producto.id==n.id&&i.push(r);return i}getMovimientosTipoMovimiento(n){let e=this.dataService.getMovimientosInventario(),i=[];for(let r of e)r.tipo_movimiento.id==n.id&&i.push(r);return i}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var ck=()=>["sku","nombre","descripcion","stock_web","stock_local","estado","opciones"];function G5(t,n){t&1&&(f(0,"th",17),g(1,"Codigo"),p()),t&2&&x("text-align","center")}function W5(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.sku," ")}}function q5(t,n){t&1&&(f(0,"th",17),g(1,"Nombre"),p()),t&2&&x("text-align","center")}function Y5(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.nombre)}}function Q5(t,n){t&1&&(f(0,"th",17),g(1,"Descripcion"),p()),t&2&&x("text-align","center")}function Z5(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.descripcion)}}function K5(t,n){t&1&&(f(0,"th",17),g(1,"Stock Web"),p()),t&2&&x("text-align","center")}function X5(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.stock_web," ")}}function J5(t,n){t&1&&(f(0,"th",17),g(1,"Stock Local"),p()),t&2&&x("text-align","center")}function e8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.stock_local," ")}}function t8(t,n){t&1&&(f(0,"th",17),g(1,"Periodo"),p()),t&2&&x("text-align","center")}function n8(t,n){if(t&1&&(f(0,"td",18),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.estado.descripcion," ")}}function i8(t,n){t&1&&(f(0,"th",19),g(1,"Ver Movimientos"),p())}function r8(t,n){if(t&1){let e=Ke();f(0,"td",18)(1,"button",20),F("click",function(){let r=Ae(e).$implicit,o=b();return Re(o.verMovimientosProducto(r))}),f(2,"mat-icon"),g(3,"article"),p()()()}}function o8(t,n){t&1&&z(0,"tr",21)}function a8(t,n){t&1&&z(0,"tr",22)}var Ih=()=>["sku","producto","fecha","lote","tipo_movimiento","cantidad"];function s8(t,n){t&1&&(f(0,"th",12),g(1,"Codigo"),p()),t&2&&x("text-align","center")}function l8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.sku," ")}}function c8(t,n){t&1&&(f(0,"th",12),g(1,"Producto"),p()),t&2&&x("text-align","center")}function d8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.nombre," ")}}function u8(t,n){t&1&&(f(0,"th",12),g(1,"Fecha"),p()),t&2&&x("text-align","center")}function f8(t,n){if(t&1&&(f(0,"td",13),g(1),Gt(2,"date"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",Fs(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function m8(t,n){t&1&&(f(0,"th",12),g(1,"Lote"),p()),t&2&&x("text-align","center")}function p8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.lote.id)}}function h8(t,n){t&1&&(f(0,"th",12),g(1,"Movimiento"),p()),t&2&&x("text-align","center")}function g8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.tipo_movimiento.descripcion)}}function v8(t,n){t&1&&(f(0,"th",12),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function _8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad," ")}}function y8(t,n){t&1&&z(0,"tr",14)}function b8(t,n){t&1&&z(0,"tr",15)}function C8(t,n){t&1&&(f(0,"th",12),g(1,"Codigo"),p()),t&2&&x("text-align","center")}function D8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.sku," ")}}function w8(t,n){t&1&&(f(0,"th",12),g(1,"Producto"),p()),t&2&&x("text-align","center")}function E8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.nombre," ")}}function x8(t,n){t&1&&(f(0,"th",12),g(1,"Fecha"),p()),t&2&&x("text-align","center")}function S8(t,n){if(t&1&&(f(0,"td",13),g(1),Gt(2,"date"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",Fs(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function M8(t,n){t&1&&(f(0,"th",12),g(1,"Lote"),p()),t&2&&x("text-align","center")}function I8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.lote.id)}}function T8(t,n){t&1&&(f(0,"th",12),g(1,"Movimiento"),p()),t&2&&x("text-align","center")}function A8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.tipo_movimiento.descripcion)}}function R8(t,n){t&1&&(f(0,"th",12),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function k8(t,n){if(t&1&&(f(0,"td",13),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad," ")}}function O8(t,n){t&1&&z(0,"tr",14)}function N8(t,n){t&1&&z(0,"tr",15)}var dk=()=>["cuenta_movimiento","tipo_movimiento","fecha","sku","producto","lote","cantidad"];function P8(t,n){t&1&&(f(0,"th",13),g(1,"Cuenta Movimiento"),p()),t&2&&x("text-align","center")}function F8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.tipo_movimiento.cuenta)}}function L8(t,n){t&1&&(f(0,"th",13),g(1,"Movimiento"),p()),t&2&&x("text-align","center")}function V8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.tipo_movimiento.descripcion)}}function j8(t,n){t&1&&(f(0,"th",13),g(1,"Codigo"),p()),t&2&&x("text-align","center")}function B8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.sku," ")}}function H8(t,n){t&1&&(f(0,"th",13),g(1,"Producto"),p()),t&2&&x("text-align","center")}function U8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.nombre," ")}}function z8(t,n){t&1&&(f(0,"th",13),g(1,"Fecha"),p()),t&2&&x("text-align","center")}function $8(t,n){if(t&1&&(f(0,"td",14),g(1),Gt(2,"date"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",Fs(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function G8(t,n){t&1&&(f(0,"th",13),g(1,"Lote"),p()),t&2&&x("text-align","center")}function W8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.lote.id)}}function q8(t,n){t&1&&(f(0,"th",13),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function Y8(t,n){if(t&1&&(f(0,"td",14),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad," ")}}function Q8(t,n){t&1&&z(0,"tr",15)}function Z8(t,n){t&1&&z(0,"tr",16)}var uk=()=>["fecha","sku","producto","lote","cantidad"];function K8(t,n){t&1&&(f(0,"th",11),g(1,"Codigo"),p()),t&2&&x("text-align","center")}function X8(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.sku," ")}}function J8(t,n){t&1&&(f(0,"th",11),g(1,"Producto"),p()),t&2&&x("text-align","center")}function eG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.producto.nombre," ")}}function tG(t,n){t&1&&(f(0,"th",11),g(1,"Fecha"),p()),t&2&&x("text-align","center")}function nG(t,n){if(t&1&&(f(0,"td",12),g(1),Gt(2,"date"),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",Fs(2,3,e.fecha,"dd/MM/yyyy hh:mm:ss a")," ")}}function iG(t,n){t&1&&(f(0,"th",11),g(1,"Lote"),p()),t&2&&x("text-align","center")}function rG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.lote.id)}}function oG(t,n){t&1&&(f(0,"th",11),g(1,"Cantidad"),p()),t&2&&x("text-align","center")}function aG(t,n){if(t&1&&(f(0,"td",12),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cantidad," ")}}function sG(t,n){t&1&&z(0,"tr",13)}function lG(t,n){t&1&&z(0,"tr",14)}function cG(t,n){if(t&1&&(f(0,"div")(1,"p"),g(2),p(),f(3,"table",1),te(4,2),N(5,K8,2,2,"th",3)(6,X8,2,3,"td",4),ne(),te(7,5),N(8,J8,2,2,"th",3)(9,eG,2,3,"td",4),ne(),te(10,6),N(11,tG,2,2,"th",3)(12,nG,3,6,"td",4),ne(),te(13,7),N(14,iG,2,2,"th",3)(15,rG,2,3,"td",4),ne(),te(16,8),N(17,oG,2,2,"th",3)(18,aG,2,3,"td",4),ne(),N(19,sG,1,0,"tr",9)(20,lG,1,0,"tr",10),p()()),t&2){let e=n.$implicit,i=b();h(2),Cr("",e.cuenta,"-",e.descripcion),h(),E("dataSource",i.movimientosPorTipo(e)),h(16),E("matHeaderRowDef",ot(5,uk)),h(),E("matRowDefColumns",ot(6,uk))}}var fk=()=>["cuenta_movimiento","tipo_movimiento"];function dG(t,n){t&1&&(f(0,"th",8),g(1,"Cuenta"),p()),t&2&&x("text-align","center")}function uG(t,n){if(t&1&&(f(0,"td",9),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.cuenta)}}function fG(t,n){t&1&&(f(0,"th",8),g(1,"Movimiento"),p()),t&2&&x("text-align","center")}function mG(t,n){if(t&1&&(f(0,"td",9),g(1),p()),t&2){let e=n.$implicit;x("text-align","center"),h(),$(" ",e.descripcion)}}function pG(t,n){t&1&&z(0,"tr",10)}function hG(t,n){t&1&&z(0,"tr",11)}var Th=class t{constructor(n){this._router=n}innerWidths="0";document=d(j);dialog=d(Zi);dataService=d(Je);productosList=S([]);tipoMovimientoList=S([]);ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarListas()}cargarListas(){this.productosList.update(n=>this.dataService.getProductos()),this.tipoMovimientoList.update(n=>this.dataService.getTiposMovimiento())}verCompras(){this.verMovimientosTipo(this.tipoMovimientoList()[this.tipoMovimientoList().findIndex(n=>n.id==1)])}verVentas(){this.verMovimientosTipo(this.tipoMovimientoList()[this.tipoMovimientoList().findIndex(n=>n.id==2)])}verMovimientosProducto(n){let e=ve.getMatDialogConf();e.data={producto:n},this.dialog.open(rC,e)}verMovimientosTipo(n){let e=ve.getMatDialogConf();e.data={tipoMovimiento:n},this.dialog.open(oC,e)}verMovimientosDiario(){let n=ve.getMatDialogConf();this.dialog.open(aC,n)}verMovimientosMayor(){let n=ve.getMatDialogConf();this.dialog.open(sC,n)}verCatalogoCuentas(){let n=ve.getMatDialogConf();this.dialog.open(lC,n)}static \u0275fac=function(e){return new(e||t)(ee(gn))};static \u0275cmp=O({type:t,selectors:[["app-inventarios"]],decls:42,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","nombre"],["matColumnDef","descripcion"],["matColumnDef","stock_web"],["matColumnDef","stock_local"],["matColumnDef","estado"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["aria-label","Ver Movimientos","matMiniFab","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),g(4," Compra de productos "),p()(),f(5,"table",2),te(6,3),N(7,G5,2,2,"th",4)(8,W5,2,3,"td",5),ne(),te(9,6),N(10,q5,2,2,"th",4)(11,Y5,2,3,"td",5),ne(),te(12,7),N(13,Q5,2,2,"th",4)(14,Z5,2,3,"td",5),ne(),te(15,8),N(16,K5,2,2,"th",4)(17,X5,2,3,"td",5),ne(),te(18,9),N(19,J5,2,2,"th",4)(20,e8,2,3,"td",5),ne(),te(21,10),N(22,t8,2,2,"th",4)(23,n8,2,3,"td",5),ne(),te(24,11),N(25,i8,2,0,"th",12)(26,r8,4,0,"td",13),ne(),N(27,o8,1,0,"tr",14)(28,a8,1,0,"tr",15),p()(),f(29,"mat-expansion-panel",1)(30,"mat-expansion-panel-header")(31,"mat-panel-description")(32,"button",16),F("click",function(){return i.verCompras()}),g(33,"Libro Compras"),p(),f(34,"button",16),F("click",function(){return i.verVentas()}),g(35,"Libro Ventas"),p(),f(36,"button",16),F("click",function(){return i.verMovimientosDiario()}),g(37,"Libro Diario"),p(),f(38,"button",16),F("click",function(){return i.verMovimientosMayor()}),g(39,"Libro Mayor"),p(),f(40,"button",16),F("click",function(){return i.verCatalogoCuentas()}),g(41,"Catalogo Cuentas"),p()()()()()),e&2&&(h(),x("overflow","auto"),h(4),E("dataSource",i.productosList()),h(22),E("matHeaderRowDef",ot(5,ck)),h(),E("matRowDefColumns",ot(6,ck)))},dependencies:[Mi,Tr,ri,oi,Ei,Ir,ut,Hp,Bn,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn],encapsulation:2})},rC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.producto.update(e=>n.producto)}innerWidths="0";document=d(j);dialogRef=d(Vt);inventarioService=d(Xd);producto=S(L.productoVacio());movimientosList=S([]);ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.inventarioService.getMovimientosProducto(this.producto())])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-detalles"]],decls:26,vars:5,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","tipo_movimiento"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"table",0),te(3,1),N(4,s8,2,2,"th",2)(5,l8,2,3,"td",3),ne(),te(6,4),N(7,c8,2,2,"th",2)(8,d8,2,3,"td",3),ne(),te(9,5),N(10,u8,2,2,"th",2)(11,f8,3,6,"td",3),ne(),te(12,6),N(13,m8,2,2,"th",2)(14,p8,2,3,"td",3),ne(),te(15,7),N(16,h8,2,2,"th",2)(17,g8,2,3,"td",3),ne(),te(18,8),N(19,v8,2,2,"th",2)(20,_8,2,3,"td",3),ne(),N(21,y8,1,0,"tr",9)(22,b8,1,0,"tr",10),p()(),f(23,"div")(24,"button",11),F("click",function(){return i.cerrarDetalles()}),g(25,"Cerrar"),p()()()),e&2&&(h(2),E("dataSource",i.movimientosList()),h(19),E("matHeaderRowDef",ot(3,Ih)),h(),E("matRowDefColumns",ot(4,Ih)))},dependencies:[qo,ut,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,nn,Hs],encapsulation:2})},oC=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.tipoMovimiento.update(e=>n.tipoMovimiento)}innerWidths="0";document=d(j);dialogRef=d(Vt);inventarioService=d(Xd);tipoMovimiento=S(L.tipoMovimientoVacio());movimientosList=S([]);ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.inventarioService.getMovimientosTipoMovimiento(this.tipoMovimiento())])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)(ee(yi))};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:28,vars:6,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","tipo_movimiento"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3),p(),f(4,"table",0),te(5,1),N(6,C8,2,2,"th",2)(7,D8,2,3,"td",3),ne(),te(8,4),N(9,w8,2,2,"th",2)(10,E8,2,3,"td",3),ne(),te(11,5),N(12,x8,2,2,"th",2)(13,S8,3,6,"td",3),ne(),te(14,6),N(15,M8,2,2,"th",2)(16,I8,2,3,"td",3),ne(),te(17,7),N(18,T8,2,2,"th",2)(19,A8,2,3,"td",3),ne(),te(20,8),N(21,R8,2,2,"th",2)(22,k8,2,3,"td",3),ne(),N(23,O8,1,0,"tr",9)(24,N8,1,0,"tr",10),p()(),f(25,"div")(26,"button",11),F("click",function(){return i.cerrarDetalles()}),g(27,"Cerrar"),p()()()),e&2&&(h(3),$("Movimientos de ",i.tipoMovimiento().descripcion),h(),E("dataSource",i.movimientosList()),h(19),E("matHeaderRowDef",ot(4,Ih)),h(),E("matRowDefColumns",ot(5,Ih)))},dependencies:[qo,ut,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,nn,Hs],encapsulation:2})},aC=class t{innerWidths="0";document=d(j);dialogRef=d(Vt);dataService=d(Je);movimientosList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.dataService.getMovimientosInventario()])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:31,vars:5,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","cuenta_movimiento"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","tipo_movimiento"],["matColumnDef","sku"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3,"Movimientos de Libro Diario"),p(),f(4,"table",0),te(5,1),N(6,P8,2,2,"th",2)(7,F8,2,3,"td",3),ne(),te(8,4),N(9,L8,2,2,"th",2)(10,V8,2,3,"td",3),ne(),te(11,5),N(12,j8,2,2,"th",2)(13,B8,2,3,"td",3),ne(),te(14,6),N(15,H8,2,2,"th",2)(16,U8,2,3,"td",3),ne(),te(17,7),N(18,z8,2,2,"th",2)(19,$8,3,6,"td",3),ne(),te(20,8),N(21,G8,2,2,"th",2)(22,W8,2,3,"td",3),ne(),te(23,9),N(24,q8,2,2,"th",2)(25,Y8,2,3,"td",3),ne(),N(26,Q8,1,0,"tr",10)(27,Z8,1,0,"tr",11),p()(),f(28,"div")(29,"button",12),F("click",function(){return i.cerrarDetalles()}),g(30,"Cerrar"),p()()()),e&2&&(h(4),E("dataSource",i.movimientosList()),h(22),E("matHeaderRowDef",ot(3,dk)),h(),E("matRowDefColumns",ot(4,dk)))},dependencies:[qo,ut,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,nn,Hs],encapsulation:2})},sC=class t{innerWidths="0";document=d(j);dialogRef=d(Vt);dataService=d(Je);movimientosList=S([]);tipoMovimientoList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.movimientosList.update(n=>[...this.dataService.getMovimientosInventario()]),this.tipoMovimientoList.update(n=>[...this.dataService.getTiposMovimiento()])}movimientosPorTipo(n){let e=this.movimientosList(),i=[];for(let r of e)r.tipo_movimiento.id==n.id&&i.push(r);return i}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:9,vars:0,consts:[["mat-stroked-button","","color","primary",3,"click"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","producto"],["matColumnDef","fecha"],["matColumnDef","lote"],["matColumnDef","cantidad"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3,"Movimientos de Libro Mayor"),p()(),Qe(4,cG,21,7,"div",null,Ye),f(6,"div")(7,"button",0),F("click",function(){return i.cerrarDetalles()}),g(8,"Cerrar"),p()()()),e&2&&(h(4),Ze(i.tipoMovimientoList()))},dependencies:[qo,ut,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,nn,Hs],encapsulation:2})},lC=class t{innerWidths="0";document=d(j);dialogRef=d(Vt);dataService=d(Je);tipoMovimientoList=S([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.tipoMovimientoList.update(n=>[...this.dataService.getTiposMovimiento()])}cerrarDetalles(){this.dialogRef.close()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["dialog-detalles-tipo"]],decls:16,vars:5,consts:[["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","cuenta_movimiento"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","tipo_movimiento"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(f(0,"mat-dialog-content")(1,"div")(2,"p"),g(3,"Catalogo de Cuentas"),p(),f(4,"table",0),te(5,1),N(6,dG,2,2,"th",2)(7,uG,2,3,"td",3),ne(),te(8,4),N(9,fG,2,2,"th",2)(10,mG,2,3,"td",3),ne(),N(11,pG,1,0,"tr",5)(12,hG,1,0,"tr",6),p()(),f(13,"div")(14,"button",7),F("click",function(){return i.cerrarDetalles()}),g(15,"Cerrar"),p()()()),e&2&&(h(4),E("dataSource",i.tipoMovimientoList()),h(7),E("matHeaderRowDef",ot(3,fk)),h(),E("matRowDefColumns",ot(4,fk)))},dependencies:[qo,ut,bn,Dn,Sn,wn,Cn,Mn,En,xn,In,Tn,nn],encapsulation:2})};var mk=[{path:"",redirectTo:"/menu",pathMatch:"full"},{path:"menu",component:wh,children:[{path:"",redirectTo:"/menu/login",pathMatch:"full"},{path:"login",component:Eh},{path:"productos",component:xh},{path:"carrito",component:Dh},{path:"ventas",component:Sh},{path:"compras",component:Mh},{path:"inventario",component:Th}]}];var gG=[Je,Hn,on,Un,Il,Ii],pk={providers:[gG,{provide:qb,useValue:{hasBackdrop:!1}},Jg(),Eb(mk,xb()),xI(EI())]};var Ah=class t{title=S("techstoresv");static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&z(0,"router-outlet")},dependencies:[Na],styles:['[_nghost-%COMP%]{--bright-blue: oklch(51.01% .274 263.83);--electric-violet: oklch(53.18% .28 296.97);--french-violet: oklch(47.66% .246 305.88);--vivid-pink: oklch(69.02% .277 332.77);--hot-red: oklch(61.42% .238 15.34);--orange-red: oklch(63.32% .24 31.68);--gray-900: oklch(19.37% .006 300.98);--gray-700: oklch(36.98% .014 302.71);--gray-400: oklch(70.9% .015 304.04);--red-to-pink-to-purple-vertical-gradient: linear-gradient( 180deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--red-to-pink-to-purple-horizontal-gradient: linear-gradient( 90deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--pill-accent: var(--bright-blue);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol;box-sizing:border-box;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;display:block;height:100dvh}h1[_ngcontent-%COMP%]{font-size:3.125rem;color:var(--gray-900);font-weight:500;line-height:100%;letter-spacing:-.125rem;margin:0;font-family:Inter Tight,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol}p[_ngcontent-%COMP%]{margin:0;color:var(--gray-700)}main[_ngcontent-%COMP%]{width:100%;min-height:100%;display:flex;justify-content:center;align-items:center;padding:1rem;box-sizing:inherit;position:relative}.angular-logo[_ngcontent-%COMP%]{max-width:9.2rem}.content[_ngcontent-%COMP%]{display:flex;justify-content:space-around;width:100%;max-width:700px;margin-bottom:3rem}.content[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin-top:1.75rem}.content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin-top:1.5rem}.divider[_ngcontent-%COMP%]{width:1px;background:var(--red-to-pink-to-purple-vertical-gradient);margin-inline:.5rem}.pill-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:start;flex-wrap:wrap;gap:1.25rem}.pill[_ngcontent-%COMP%]{display:flex;align-items:center;--pill-accent: var(--bright-blue);background:color-mix(in srgb,var(--pill-accent) 5%,transparent);color:var(--pill-accent);padding-inline:.75rem;padding-block:.375rem;border-radius:2.75rem;border:0;transition:background .3s ease;font-family:var(--inter-font);font-size:.875rem;font-style:normal;font-weight:500;line-height:1.4rem;letter-spacing:-.00875rem;text-decoration:none;white-space:nowrap}.pill[_ngcontent-%COMP%]:hover{background:color-mix(in srgb,var(--pill-accent) 15%,transparent)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+1){--pill-accent: var(--bright-blue)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+2){--pill-accent: var(--electric-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+3){--pill-accent: var(--french-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+4), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+5), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+6){--pill-accent: var(--hot-red)}.pill-group[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{margin-inline-start:.25rem}.social-links[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.73rem;margin-top:1.5rem}.social-links[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{transition:fill .3s ease;fill:var(--gray-400)}.social-links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover   svg[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{fill:var(--gray-900)}@media screen and (max-width:650px){.content[_ngcontent-%COMP%]{flex-direction:column;width:max-content}.divider[_ngcontent-%COMP%]{height:1px;width:100%;background:var(--red-to-pink-to-purple-horizontal-gradient);margin-block:1.5rem}}']})};Hy(Ah,pk).catch(t=>console.error(t));
