var hR=Object.defineProperty,mR=Object.defineProperties;var gR=Object.getOwnPropertyDescriptors;var Td=Object.getOwnPropertySymbols;var E0=Object.prototype.hasOwnProperty,x0=Object.prototype.propertyIsEnumerable;var w0=(t,n,e)=>n in t?hR(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,D=(t,n)=>{for(var e in n||={})E0.call(n,e)&&w0(t,e,n[e]);if(Td)for(var e of Td(n))x0.call(n,e)&&w0(t,e,n[e]);return t},se=(t,n)=>mR(t,gR(n));var S0=(t,n)=>{var e={};for(var i in t)E0.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&Td)for(var i of Td(t))n.indexOf(i)<0&&x0.call(t,i)&&(e[i]=t[i]);return e};var jt=null,Ad=!1,Kh=1,vR=null,ht=Symbol("SIGNAL");function ie(t){let n=jt;return jt=t,n}function Rd(){return jt}var Ir={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Mr(t){if(Ad)throw new Error("");if(jt===null)return;jt.consumerOnSignalRead(t);let n=jt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=jt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:jt.producers,e!==void 0&&e.producer===t)){jt.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===jt&&(!i||yR(r,jt)))return;let o=Ea(jt),a={producer:t,consumer:jt,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};jt.producersTail=a,n!==void 0?n.nextProducer=a:jt.producers=a,o&&A0(t,a)}function I0(){Kh++}function Co(t){if(!(Ea(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Kh)){if(!t.producerMustRecompute(t)&&!wa(t)){Da(t);return}t.producerRecomputeValue(t),Da(t)}}function Xh(t){if(t.consumers===void 0)return;let n=Ad;Ad=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||_R(i)}}finally{Ad=n}}function Jh(){return jt?.consumerAllowSignalWrites!==!1}function _R(t){t.dirty=!0,Xh(t),t.consumerMarkedDirty?.(t)}function Da(t){t.dirty=!1,t.lastCleanEpoch=Kh}function Zi(t){return t&&M0(t),ie(t)}function M0(t){t.producersTail=void 0,t.recomputing=!0}function Tr(t,n){ie(n),t&&T0(t)}function T0(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ea(t))do e=em(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function wa(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Co(e),i!==e.version))return!0}return!1}function Ar(t){if(Ea(t)){let n=t.producers;for(;n!==void 0;)n=em(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function A0(t,n){let e=t.consumersTail,i=Ea(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)A0(r.producer,r)}function em(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ea(n)){let o=n.producers;for(;o!==void 0;)o=em(o)}return e}function Ea(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function dl(t){vR?.(t)}function yR(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function ul(t,n){return Object.is(t,n)}function fl(t,n){let e=Object.create(bR);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Co(e),Mr(e),e.value===yi)throw e.error;return e.value};return i[ht]=e,dl(e),i}var yo=Symbol("UNSET"),bo=Symbol("COMPUTING"),yi=Symbol("ERRORED"),bR=se(D({},Ir),{value:yo,dirty:!0,error:null,equal:ul,kind:"computed",producerMustRecompute(t){return t.value===yo||t.value===bo},producerRecomputeValue(t){if(t.value===bo)throw new Error("");let n=t.value;t.value=bo;let e=Zi(t),i,r=!1;try{i=t.computation(),ie(null),r=n!==yo&&n!==yi&&i!==yi&&t.equal(n,i)}catch(o){i=yi,t.error=o}finally{Tr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function CR(){throw new Error}var R0=CR;function k0(t){R0(t)}function tm(t){R0=t}var DR=null;function nm(t,n){let e=Object.create(pl);e.value=t,n!==void 0&&(e.equal=n);let i=()=>O0(e);return i[ht]=e,dl(e),[i,a=>Do(e,a),a=>kd(e,a)]}function O0(t){return Mr(t),t.value}function Do(t,n){Jh()||k0(t),t.equal(t.value,n)||(t.value=n,wR(t))}function kd(t,n){Jh()||k0(t),Do(t,n(t.value))}var pl=se(D({},Ir),{equal:ul,value:void 0,kind:"signal"});function wR(t){t.version++,I0(),Xh(t),DR?.(t)}var im=se(D({},Ir),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function rm(t){if(t.dirty=!1,t.version>0&&!wa(t))return;t.version++;let n=Zi(t);try{t.cleanup(),t.fn()}finally{Tr(t,n)}}function be(t){return typeof t=="function"}function xa(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Od=xa(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function wo(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var le=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(be(i))try{i()}catch(o){n=o instanceof Od?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{N0(o)}catch(a){n=n??[],a instanceof Od?n=[...n,...a.errors]:n.push(a)}}if(n)throw new Od(n)}}add(n){var e;if(n&&n!==this)if(this.closed)N0(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&wo(e,n)}remove(n){let{_finalizers:e}=this;e&&wo(e,n),n instanceof t&&n._removeParent(this)}};le.EMPTY=(()=>{let t=new le;return t.closed=!0,t})();var om=le.EMPTY;function Nd(t){return t instanceof le||t&&"closed"in t&&be(t.remove)&&be(t.add)&&be(t.unsubscribe)}function N0(t){be(t)?t():t.unsubscribe()}var qn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Sa={setTimeout(t,n,...e){let{delegate:i}=Sa;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Sa;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Pd(t){Sa.setTimeout(()=>{let{onUnhandledError:n}=qn;if(n)n(t);else throw t})}function hl(){}var P0=am("C",void 0,void 0);function F0(t){return am("E",void 0,t)}function L0(t){return am("N",t,void 0)}function am(t,n,e){return{kind:t,value:n,error:e}}var Eo=null;function Ia(t){if(qn.useDeprecatedSynchronousErrorHandling){let n=!Eo;if(n&&(Eo={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Eo;if(Eo=null,e)throw i}}else t()}function V0(t){qn.useDeprecatedSynchronousErrorHandling&&Eo&&(Eo.errorThrown=!0,Eo.error=t)}var xo=class extends le{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Nd(n)&&n.add(this)):this.destination=SR}static create(n,e,i){return new Ki(n,e,i)}next(n){this.isStopped?lm(L0(n),this):this._next(n)}error(n){this.isStopped?lm(F0(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?lm(P0,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},ER=Function.prototype.bind;function sm(t,n){return ER.call(t,n)}var cm=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Fd(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Fd(i)}else Fd(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Fd(e)}}},Ki=class extends xo{constructor(n,e,i){super();let r;if(be(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&qn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&sm(n.next,o),error:n.error&&sm(n.error,o),complete:n.complete&&sm(n.complete,o)}):r=n}this.destination=new cm(r)}};function Fd(t){qn.useDeprecatedSynchronousErrorHandling?V0(t):Pd(t)}function xR(t){throw t}function lm(t,n){let{onStoppedNotification:e}=qn;e&&Sa.setTimeout(()=>e(t,n))}var SR={closed:!0,next:hl,error:xR,complete:hl};var Ma=typeof Symbol=="function"&&Symbol.observable||"@@observable";function un(t){return t}function dm(...t){return um(t)}function um(t){return t.length===0?un:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var he=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=MR(e)?e:new Ki(e,i,r);return Ia(()=>{let{operator:a,source:s}=this;o.add(a?a.call(o,s):s?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=j0(i),new i((r,o)=>{let a=new Ki({next:s=>{try{e(s)}catch(l){o(l),a.unsubscribe()}},error:o,complete:r});this.subscribe(a)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Ma](){return this}pipe(...e){return um(e)(this)}toPromise(e){return e=j0(e),new e((i,r)=>{let o;this.subscribe(a=>o=a,a=>r(a),()=>i(o))})}}return t.create=n=>new t(n),t})();function j0(t){var n;return(n=t??qn.Promise)!==null&&n!==void 0?n:Promise}function IR(t){return t&&be(t.next)&&be(t.error)&&be(t.complete)}function MR(t){return t&&t instanceof xo||IR(t)&&Nd(t)}function fm(t){return be(t?.lift)}function ve(t){return n=>{if(fm(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function me(t,n,e,i,r){return new pm(t,n,e,i,r)}var pm=class extends xo{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};function B0(){return ve((t,n)=>{let e=null;t._refCount++;let i=me(n,void 0,void 0,void 0,()=>{if(!t||t._refCount<=0||0<--t._refCount){e=null;return}let r=t._connection,o=e;e=null,r&&(!o||r===o)&&r.unsubscribe(),n.unsubscribe()});t.subscribe(i),i.closed||(e=t.connect())})}var ml=class extends he{constructor(n,e){super(),this.source=n,this.subjectFactory=e,this._subject=null,this._refCount=0,this._connection=null,fm(n)&&(this.lift=n.lift)}_subscribe(n){return this.getSubject().subscribe(n)}getSubject(){let n=this._subject;return(!n||n.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:n}=this;this._subject=this._connection=null,n?.unsubscribe()}connect(){let n=this._connection;if(!n){n=this._connection=new le;let e=this.getSubject();n.add(this.source.subscribe(me(e,void 0,()=>{this._teardown(),e.complete()},i=>{this._teardown(),e.error(i)},()=>this._teardown()))),n.closed&&(this._connection=null,n=le.EMPTY)}return n}refCount(){return B0()(this)}};var Ta={schedule(t){let n=requestAnimationFrame,e=cancelAnimationFrame,{delegate:i}=Ta;i&&(n=i.requestAnimationFrame,e=i.cancelAnimationFrame);let r=n(o=>{e=void 0,t(o)});return new le(()=>e?.(r))},requestAnimationFrame(...t){let{delegate:n}=Ta;return(n?.requestAnimationFrame||requestAnimationFrame)(...t)},cancelAnimationFrame(...t){let{delegate:n}=Ta;return(n?.cancelAnimationFrame||cancelAnimationFrame)(...t)},delegate:void 0};var H0=xa(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var S=(()=>{class t extends he{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Ld(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new H0}next(e){Ia(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Ia(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Ia(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?om:(this.currentObservers=null,o.push(e),new le(()=>{this.currentObservers=null,wo(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new he;return e.source=this,e}}return t.create=(n,e)=>new Ld(n,e),t})(),Ld=class extends S{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:om}};var mt=class extends S{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var gl={now(){return(gl.delegate||Date).now()},delegate:void 0};var Rr=class extends S{constructor(n=1/0,e=1/0,i=gl){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var Vd=class extends le{constructor(n,e){super()}schedule(n,e=0){return this}};var vl={setInterval(t,n,...e){let{delegate:i}=vl;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=vl;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var kr=class extends Vd{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return vl.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&vl.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,wo(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var TR=1,hm,mm={};function U0(t){return t in mm?(delete mm[t],!0):!1}var z0={setImmediate(t){let n=TR++;return mm[n]=!0,hm||(hm=Promise.resolve()),hm.then(()=>U0(n)&&t()),n},clearImmediate(t){U0(t)}};var{setImmediate:AR,clearImmediate:RR}=z0,_l={setImmediate(...t){let{delegate:n}=_l;return(n?.setImmediate||AR)(...t)},clearImmediate(t){let{delegate:n}=_l;return(n?.clearImmediate||RR)(t)},delegate:void 0};var jd=class extends kr{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=_l.setImmediate(n.flush.bind(n,void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(_l.clearImmediate(e),n._scheduled===e&&(n._scheduled=void 0))}};var Aa=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Aa.now=gl.now;var Or=class extends Aa{constructor(n,e=Aa.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Bd=class extends Or{flush(n){this._active=!0;let e=this._scheduled;this._scheduled=void 0;let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var Hd=new Bd(jd);var yl=new Or(kr),$0=yl;var Ud=class extends kr{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=Ta.requestAnimationFrame(()=>n.flush(void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&e===n._scheduled&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(Ta.cancelAnimationFrame(e),n._scheduled=void 0)}};var zd=class extends Or{flush(n){this._active=!0;let e;n?e=n.id:(e=this._scheduled,this._scheduled=void 0);let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var $d=new zd(Ud);var ot=new he(t=>t.complete());function Gd(t){return t&&be(t.schedule)}function gm(t){return t[t.length-1]}function Wd(t){return be(gm(t))?t.pop():void 0}function bi(t){return Gd(gm(t))?t.pop():void 0}function G0(t,n){return typeof gm(t)=="number"?t.pop():n}function q0(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(u){try{c(i.next(u))}catch(f){a(f)}}function l(u){try{c(i.throw(u))}catch(f){a(f)}}function c(u){u.done?o(u.value):r(u.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function W0(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function So(t){return this instanceof So?(this.v=t,this):new So(t)}function Y0(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(h){return function(_){return Promise.resolve(_).then(h,f)}}function s(h,_){i[h]&&(r[h]=function(x){return new Promise(function(T,N){o.push([h,x,T,N])>1||l(h,x)})},_&&(r[h]=_(r[h])))}function l(h,_){try{c(i[h](_))}catch(x){p(o[0][3],x)}}function c(h){h.value instanceof So?Promise.resolve(h.value.v).then(u,f):p(o[0][2],h)}function u(h){l("next",h)}function f(h){l("throw",h)}function p(h,_){h(_),o.shift(),o.length&&l(o[0][0],o[0][1])}}function Q0(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof W0=="function"?W0(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var qd=t=>t&&typeof t.length=="number"&&typeof t!="function";function Yd(t){return be(t?.then)}function Qd(t){return be(t[Ma])}function Zd(t){return Symbol.asyncIterator&&be(t?.[Symbol.asyncIterator])}function Kd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function kR(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Xd=kR();function Jd(t){return be(t?.[Xd])}function eu(t){return Y0(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield So(e.read());if(r)return yield So(void 0);yield yield So(i)}}finally{e.releaseLock()}})}function tu(t){return be(t?.getReader)}function We(t){if(t instanceof he)return t;if(t!=null){if(Qd(t))return OR(t);if(qd(t))return NR(t);if(Yd(t))return PR(t);if(Zd(t))return Z0(t);if(Jd(t))return FR(t);if(tu(t))return LR(t)}throw Kd(t)}function OR(t){return new he(n=>{let e=t[Ma]();if(be(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function NR(t){return new he(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function PR(t){return new he(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Pd)})}function FR(t){return new he(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Z0(t){return new he(n=>{VR(t,n).catch(e=>n.error(e))})}function LR(t){return Z0(eu(t))}function VR(t,n){var e,i,r,o;return q0(this,void 0,void 0,function*(){try{for(e=Q0(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function rn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function nu(t,n=0){return ve((e,i)=>{e.subscribe(me(i,r=>rn(i,t,()=>i.next(r),n),()=>rn(i,t,()=>i.complete(),n),r=>rn(i,t,()=>i.error(r),n)))})}function iu(t,n=0){return ve((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function K0(t,n){return We(t).pipe(iu(n),nu(n))}function X0(t,n){return We(t).pipe(iu(n),nu(n))}function J0(t,n){return new he(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function eC(t,n){return new he(e=>{let i;return rn(e,n,()=>{i=t[Xd](),rn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>be(i?.return)&&i.return()})}function ru(t,n){if(!t)throw new Error("Iterable cannot be null");return new he(e=>{rn(e,n,()=>{let i=t[Symbol.asyncIterator]();rn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function tC(t,n){return ru(eu(t),n)}function nC(t,n){if(t!=null){if(Qd(t))return K0(t,n);if(qd(t))return J0(t,n);if(Yd(t))return X0(t,n);if(Zd(t))return ru(t,n);if(Jd(t))return eC(t,n);if(tu(t))return tC(t,n)}throw Kd(t)}function Ke(t,n){return n?nC(t,n):We(t)}function Q(...t){let n=bi(t);return Ke(t,n)}function bl(t,n){let e=be(t)?t:()=>t,i=r=>r.error(e());return new he(n?r=>n.schedule(i,0,r):i)}function Io(t){return!!t&&(t instanceof he||be(t.lift)&&be(t.subscribe))}var Mo=xa(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function iC(t){return t instanceof Date&&!isNaN(t)}function ce(t,n){return ve((e,i)=>{let r=0;e.subscribe(me(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:jR}=Array;function BR(t,n){return jR(n)?t(...n):t(n)}function ou(t){return ce(n=>BR(t,n))}var{isArray:HR}=Array,{getPrototypeOf:UR,prototype:zR,keys:$R}=Object;function au(t){if(t.length===1){let n=t[0];if(HR(n))return{args:n,keys:null};if(GR(n)){let e=$R(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function GR(t){return t&&typeof t=="object"&&UR(t)===zR}function su(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Ci(...t){let n=bi(t),e=Wd(t),{args:i,keys:r}=au(t);if(i.length===0)return Ke([],n);let o=new he(WR(i,n,r?a=>su(r,a):un));return e?o.pipe(ou(e)):o}function WR(t,n,e=un){return i=>{rC(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)rC(n,()=>{let c=Ke(t[l],n),u=!1;c.subscribe(me(i,f=>{o[l]=f,u||(u=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function rC(t,n,e){t?rn(e,t,n):n()}function oC(t,n,e,i,r,o,a,s){let l=[],c=0,u=0,f=!1,p=()=>{f&&!l.length&&!c&&n.complete()},h=x=>c<i?_(x):l.push(x),_=x=>{o&&n.next(x),c++;let T=!1;We(e(x,u++)).subscribe(me(n,N=>{r?.(N),o?h(N):n.next(N)},()=>{T=!0},void 0,()=>{if(T)try{for(c--;l.length&&c<i;){let N=l.shift();a?rn(n,a,()=>_(N)):_(N)}p()}catch(N){n.error(N)}}))};return t.subscribe(me(n,h,()=>{f=!0,p()})),()=>{s?.()}}function Bt(t,n,e=1/0){return be(n)?Bt((i,r)=>ce((o,a)=>n(i,o,r,a))(We(t(i,r))),e):(typeof n=="number"&&(e=n),ve((i,r)=>oC(i,r,t,e)))}function Nr(t=1/0){return Bt(un,t)}function aC(){return Nr(1)}function Pr(...t){return aC()(Ke(t,bi(t)))}function Yn(t){return new he(n=>{We(t()).subscribe(n)})}function Cl(...t){let n=Wd(t),{args:e,keys:i}=au(t),r=new he(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let u=0;u<a;u++){let f=!1;We(e[u]).subscribe(me(o,p=>{f||(f=!0,c--),s[u]=p},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?su(i,s):s),o.complete())}))}});return n?r.pipe(ou(n)):r}function sC(t=0,n,e=$0){let i=-1;return n!=null&&(Gd(n)?e=n:i=n),new he(r=>{let o=iC(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Gt(...t){let n=bi(t),e=G0(t,1/0),i=t;return i.length?i.length===1?We(i[0]):Nr(e)(Ke(i,n)):ot}function Ee(t,n){return ve((e,i)=>{let r=0;e.subscribe(me(i,o=>t.call(n,o,r++)&&i.next(o)))})}function lC(t){return ve((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(me(e,c=>{i=!0,r=c,o||We(t(c)).subscribe(o=me(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function Ra(t,n=yl){return lC(()=>sC(t,n))}function Fr(t){return ve((n,e)=>{let i=null,r=!1,o;i=n.subscribe(me(e,void 0,void 0,a=>{o=We(t(a,Fr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function To(t,n){return be(n)?Bt(t,n,1):Bt(t,1)}function Dl(t,n=yl){return ve((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}s()}e.subscribe(me(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function cC(t){return ve((n,e)=>{let i=!1;n.subscribe(me(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Xe(t){return t<=0?()=>ot:ve((n,e)=>{let i=0;n.subscribe(me(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function lu(t,n=un){return t=t??qR,ve((e,i)=>{let r,o=!0;e.subscribe(me(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function qR(t,n){return t===n}function dC(t=YR){return ve((n,e)=>{let i=!1;n.subscribe(me(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function YR(){return new Mo}function Ao(t){return ve((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Xi(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Ee((r,o)=>t(r,o,i)):un,Xe(1),e?cC(n):dC(()=>new Mo))}function cu(t){return t<=0?()=>ot:ve((n,e)=>{let i=[];n.subscribe(me(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function du(){return ve((t,n)=>{let e,i=!1;t.subscribe(me(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function wl(t={}){let{connector:n=()=>new S,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,u=!1,f=!1,p=()=>{s?.unsubscribe(),s=void 0},h=()=>{p(),a=l=void 0,u=f=!1},_=()=>{let x=a;h(),x?.unsubscribe()};return ve((x,T)=>{c++,!f&&!u&&p();let N=l=l??n();T.add(()=>{c--,c===0&&!f&&!u&&(s=vm(_,r))}),N.subscribe(T),!a&&c>0&&(a=new Ki({next:pe=>N.next(pe),error:pe=>{f=!0,p(),s=vm(h,e,pe),N.error(pe)},complete:()=>{u=!0,p(),s=vm(h,i),N.complete()}}),We(x).subscribe(a))})(o)}}function vm(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Ki({next:()=>{i.unsubscribe(),t()}});return We(n(...e)).subscribe(i)}function uu(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,wl({connector:()=>new Rr(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function El(t){return Ee((n,e)=>t<=e)}function $e(...t){let n=bi(t);return ve((e,i)=>{(n?Pr(t,e,n):Pr(t,e)).subscribe(i)})}function at(t,n){return ve((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(me(i,l=>{r?.unsubscribe();let c=0,u=o++;We(t(l,u)).subscribe(r=me(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function xe(t){return ve((n,e)=>{We(t).subscribe(me(e,()=>e.complete(),hl)),!e.closed&&n.subscribe(e)})}function _m(t,n=!1){return ve((e,i)=>{let r=0;e.subscribe(me(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function Rt(t,n,e){let i=be(t)||n||e?{next:t,error:n,complete:e}:t;return i?ve((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(me(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;s=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;s=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;s&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):un}var ym;function fu(){return ym}function Di(t){let n=ym;return ym=t,n}var uC=Symbol("NotFound");function ka(t){return t===uC||t?.name==="\u0275NotFound"}function bm(t,n,e){let i=Object.create(QR);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Co(i),Mr(i),i.value===yi)throw i.error;return i.value};return o[ht]=i,dl(i),o}function fC(t,n){Co(t),Do(t,n),Da(t)}function pC(t,n){if(Co(t),t.value===yi)throw t.error;kd(t,n),Da(t)}var QR=se(D({},Ir),{value:yo,dirty:!0,error:null,equal:ul,kind:"linkedSignal",producerMustRecompute(t){return t.value===yo||t.value===bo},producerRecomputeValue(t){if(t.value===bo)throw new Error("");let n=t.value;t.value=bo;let e=Zi(t),i,r=!1;try{let o=t.source(),a=n!==yo&&n!==yi,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,ie(null),r=a&&i!==yi&&t.equal(n,i)}catch(o){i=yi,t.error=o}finally{Tr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function hC(t){let n=ie(null);try{return t()}finally{ie(n)}}var yu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",k=class extends Error{code;constructor(n,e){super(wi(n,e)),this.code=n}};function ZR(t){return`NG0${Math.abs(t)}`}function wi(t,n){return`${ZR(t)}${n?": "+n:""}`}var In=globalThis;function Ue(t){for(let n in t)if(t[n]===Ue)return n;throw Error("")}function yC(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Rl(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Rl).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function bu(t,n){return t?n?`${t} ${n}`:t:n||""}var KR=Ue({__forward_ref__:Ue});function on(t){return t.__forward_ref__=on,t}function It(t){return Om(t)?t():t}function Om(t){return typeof t=="function"&&t.hasOwnProperty(KR)&&t.__forward_ref__===on}function w(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function P(t){return{providers:t.providers||[],imports:t.imports||[]}}function kl(t){return XR(t,Cu)}function Nm(t){return kl(t)!==null}function XR(t,n){return t.hasOwnProperty(n)&&t[n]||null}function JR(t){let n=t?.[Cu]??null;return n||null}function Dm(t){return t&&t.hasOwnProperty(hu)?t[hu]:null}var Cu=Ue({\u0275prov:Ue}),hu=Ue({\u0275inj:Ue}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=w({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Pm(t){return t&&!!t.\u0275providers}var Fm=Ue({\u0275cmp:Ue}),Lm=Ue({\u0275dir:Ue}),Vm=Ue({\u0275pipe:Ue}),jm=Ue({\u0275mod:Ue}),Sl=Ue({\u0275fac:Ue}),Po=Ue({__NG_ELEMENT_ID__:Ue}),mC=Ue({__NG_ENV_ID__:Ue});function Bm(t){return Du(t,"@NgModule"),t[jm]||null}function Zn(t){return Du(t,"@Component"),t[Fm]||null}function Ol(t){return Du(t,"@Directive"),t[Lm]||null}function Hm(t){return Du(t,"@Pipe"),t[Vm]||null}function Du(t,n){if(t==null)throw new k(-919,!1)}function Na(t){return typeof t=="string"?t:t==null?"":String(t)}var bC=Ue({ngErrorCode:Ue}),ek=Ue({ngErrorMessage:Ue}),tk=Ue({ngTokenPath:Ue});function Um(t,n){return CC("",-200,n)}function wu(t,n){throw new k(-201,!1)}function CC(t,n,e){let i=new k(n,t);return i[bC]=n,i[ek]=t,e&&(i[tk]=e),i}function nk(t){return t[bC]}var wm;function DC(){return wm}function Wt(t){let n=wm;return wm=t,n}function zm(t,n,e){let i=kl(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;wu(t,"")}var ik={},Ro=ik,rk="__NG_DI_FLAG__",Em=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=ko(e)||0;try{return this.injector.get(n,i&8?null:Ro,i)}catch(r){if(ka(r))return r;throw r}}};function ok(t,n=0){let e=fu();if(e===void 0)throw new k(-203,!1);if(e===null)return zm(t,void 0,n);{let i=ak(n),r=e.retrieve(t,i);if(ka(r)){if(i.optional)return null;throw r}return r}}function ne(t,n=0){return(DC()||ok)(It(t),n)}function d(t,n){return ne(t,ko(n))}function ko(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function ak(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function xm(t){let n=[];for(let e=0;e<t.length;e++){let i=It(t[e]);if(Array.isArray(i)){if(i.length===0)throw new k(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=sk(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(ne(r,o))}else n.push(ne(i))}return n}function sk(t){return t[rk]}function Lr(t,n){let e=t.hasOwnProperty(Sl);return e?t[Sl]:null}function wC(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function EC(t){return t.flat(Number.POSITIVE_INFINITY)}function Eu(t,n){t.forEach(e=>Array.isArray(e)?Eu(e,n):n(e))}function $m(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Nl(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function xC(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function SC(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function xu(t,n,e){let i=Pa(t,n);return i>=0?t[i|1]=e:(i=~i,SC(t,i,n,e)),i}function Su(t,n){let e=Pa(t,n);if(e>=0)return t[e|1]}function Pa(t,n){return lk(t,n,1)}function lk(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Kn={},Ht=[],Ei=new y(""),Gm=new y("",-1),Wm=new y(""),Il=class{get(n,e=Ro){if(e===Ro){let r=CC("",-201);throw r.name="\u0275NotFound",r}return e}};function xi(t){return{\u0275providers:t}}function IC(t){return xi([{provide:Ei,multi:!0,useValue:t}])}function MC(...t){return{\u0275providers:Iu(!0,t),\u0275fromNgModule:!0}}function Iu(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return Eu(n,a=>{let s=a;mu(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&TC(r,o),e}function TC(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];qm(r,o=>{n(o,i)})}}function mu(t,n,e,i){if(t=It(t),!t)return!1;let r=null,o=Dm(t),a=!o&&Zn(t);if(!o&&!a){let l=t.ngModule;if(o=Dm(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)mu(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;Eu(o.imports,u=>{mu(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&TC(c,n)}if(!s){let c=Lr(r)||(()=>new r);n({provide:r,useFactory:c,deps:Ht},r),n({provide:Wm,useValue:r,multi:!0},r),n({provide:Ei,useValue:()=>ne(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;qm(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function qm(t,n){for(let e of t)Pm(e)&&(e=e.\u0275providers),Array.isArray(e)?qm(e,n):n(e)}var ck=Ue({provide:String,useValue:Ue});function AC(t){return t!==null&&typeof t=="object"&&ck in t}function dk(t){return!!(t&&t.useExisting)}function uk(t){return!!(t&&t.useFactory)}function Oo(t){return typeof t=="function"}function RC(t){return!!t.useClass}var Pl=new y(""),pu={},gC={},Cm;function Fa(){return Cm===void 0&&(Cm=new Il),Cm}var ze=class{},No=class extends ze{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Im(n,a=>this.processProvider(a)),this.records.set(Gm,Oa(void 0,this)),r.has("environment")&&this.records.set(ze,Oa(void 0,this));let o=this.records.get(Pl);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Wm,Ht,{self:!0}))}retrieve(n,e){let i=ko(e)||0;try{return this.get(n,Ro,i)}catch(r){if(ka(r))return r;throw r}}destroy(){xl(this),this._destroyed=!0;let n=ie(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ie(n)}}onDestroy(n){return xl(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){xl(this);let e=Di(this),i=Wt(void 0),r;try{return n()}finally{Di(e),Wt(i)}}get(n,e=Ro,i){if(xl(this),n.hasOwnProperty(mC))return n[mC](this);let r=ko(i),o,a=Di(this),s=Wt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=gk(n)&&kl(n);u&&this.injectableDefInScope(u)?c=Oa(Sm(n),pu):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Fa():this.parent;return e=r&8&&e===Ro?null:e,l.get(n,e)}catch(l){let c=nk(l);throw c===-200||c===-201?new k(c,null):l}finally{Wt(s),Di(a)}}resolveInjectorInitializers(){let n=ie(null),e=Di(this),i=Wt(void 0),r;try{let o=this.get(Ei,Ht,{self:!0});for(let a of o)a()}finally{Di(e),Wt(i),ie(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=It(n);let e=Oo(n)?n:It(n&&n.provide),i=pk(n);if(!Oo(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Oa(void 0,pu,!0),r.factory=()=>xm(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ie(null);try{if(e.value===gC)throw Um("");return e.value===pu&&(e.value=gC,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&mk(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ie(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=It(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Sm(t){let n=kl(t),e=n!==null?n.factory:Lr(t);if(e!==null)return e;if(t instanceof y)throw new k(-204,!1);if(t instanceof Function)return fk(t);throw new k(-204,!1)}function fk(t){if(t.length>0)throw new k(-204,!1);let e=JR(t);return e!==null?()=>e.factory(t):()=>new t}function pk(t){if(AC(t))return Oa(void 0,t.useValue);{let n=Ym(t);return Oa(n,pu)}}function Ym(t,n,e){let i;if(Oo(t)){let r=It(t);return Lr(r)||Sm(r)}else if(AC(t))i=()=>It(t.useValue);else if(uk(t))i=()=>t.useFactory(...xm(t.deps||[]));else if(dk(t))i=(r,o)=>ne(It(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=It(t&&(t.useClass||t.provide));if(hk(t))i=()=>new r(...xm(t.deps));else return Lr(r)||Sm(r)}return i}function xl(t){if(t.destroyed)throw new k(-205,!1)}function Oa(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function hk(t){return!!t.deps}function mk(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function gk(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Im(t,n){for(let e of t)Array.isArray(e)?Im(e,n):e&&Pm(e)?Im(e.\u0275providers,n):n(e)}function kt(t,n){let e;t instanceof No?(xl(t),e=t):e=new Em(t);let i,r=Di(e),o=Wt(void 0);try{return n()}finally{Di(r),Wt(o)}}function kC(){return DC()!==void 0||fu()!=null}var Yt=0,Z=1,de=2,gt=3,Mn=4,Qt=5,Zt=6,La=7,vt=8,fn=9,Xn=10,ke=11,Va=12,Qm=13,Fo=14,Nt=15,Br=16,Lo=17,Si=18,er=19,Zm=20,Ji=21,Mu=22,Vr=23,pn=24,Vo=25,Hr=26,Oe=27,OC=1,Jn=6,Ii=7,Fl=8,jo=9,st=10;function Tn(t){return Array.isArray(t)&&typeof t[OC]=="object"}function hn(t){return Array.isArray(t)&&t[OC]===!0}function Km(t){return(t.flags&4)!==0}function Mi(t){return t.componentOffset>-1}function ja(t){return(t.flags&1)===1}function ei(t){return!!t.template}function Bo(t){return(t[de]&512)!==0}function Ur(t){return(t[de]&256)===256}var Xm="svg",NC="math";function mn(t){for(;Array.isArray(t);)t=t[Yt];return t}function Jm(t,n){return mn(n[t])}function An(t,n){return mn(n[t.index])}function Ba(t,n){return t.data[n]}function eg(t,n){return t[n]}function tg(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Rn(t,n){let e=n[t];return Tn(e)?e:e[Yt]}function PC(t){return(t[de]&4)===4}function Tu(t){return(t[de]&128)===128}function FC(t){return hn(t[gt])}function gn(t,n){return n==null?null:t[n]}function ng(t){t[Lo]=0}function ig(t){t[de]&1024||(t[de]|=1024,Tu(t)&&Ho(t))}function LC(t,n){for(;t>0;)n=n[Fo],t--;return n}function Ll(t){return!!(t[de]&9216||t[pn]?.dirty)}function Au(t){t[Xn].changeDetectionScheduler?.notify(8),t[de]&64&&(t[de]|=1024),Ll(t)&&Ho(t)}function Ho(t){t[Xn].changeDetectionScheduler?.notify(0);let n=jr(t);for(;n!==null&&!(n[de]&8192||(n[de]|=8192,!Tu(n)));)n=jr(n)}function rg(t,n){if(Ur(t))throw new k(911,!1);t[Ji]===null&&(t[Ji]=[]),t[Ji].push(n)}function VC(t,n){if(t[Ji]===null)return;let e=t[Ji].indexOf(n);e!==-1&&t[Ji].splice(e,1)}function jr(t){let n=t[gt];return hn(n)?n[gt]:n}function og(t){return t[La]??=[]}function ag(t){return t.cleanup??=[]}function jC(t,n,e,i){let r=og(n);r.push(e),t.firstCreatePass&&ag(t).push(i,r.length-1)}var Ce={lFrame:XC(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Mm=!1;function BC(){return Ce.lFrame.elementDepthCount}function HC(){Ce.lFrame.elementDepthCount++}function sg(){Ce.lFrame.elementDepthCount--}function Ru(){return Ce.bindingsEnabled}function ku(){return Ce.skipHydrationRootTNode!==null}function lg(t){return Ce.skipHydrationRootTNode===t}function UC(t){Ce.skipHydrationRootTNode=t}function cg(){Ce.skipHydrationRootTNode=null}function re(){return Ce.lFrame.lView}function Ye(){return Ce.lFrame.tView}function je(t){return Ce.lFrame.contextLView=t,t[vt]}function Be(t){return Ce.lFrame.contextLView=null,t}function Mt(){let t=dg();for(;t!==null&&t.type===64;)t=t.parent;return t}function dg(){return Ce.lFrame.currentTNode}function zC(){let t=Ce.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Ha(t,n){let e=Ce.lFrame;e.currentTNode=t,e.isParent=n}function ug(){return Ce.lFrame.isParent}function fg(){Ce.lFrame.isParent=!1}function $C(){return Ce.lFrame.contextLView}function pg(){return Mm}function Ml(t){let n=Mm;return Mm=t,n}function Vl(){let t=Ce.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function GC(){return Ce.lFrame.bindingIndex}function WC(t){return Ce.lFrame.bindingIndex=t}function zr(){return Ce.lFrame.bindingIndex++}function Ou(t){let n=Ce.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function qC(){return Ce.lFrame.inI18n}function YC(t,n){let e=Ce.lFrame;e.bindingIndex=e.bindingRootIndex=t,Nu(n)}function QC(){return Ce.lFrame.currentDirectiveIndex}function Nu(t){Ce.lFrame.currentDirectiveIndex=t}function ZC(t){let n=Ce.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Pu(){return Ce.lFrame.currentQueryIndex}function jl(t){Ce.lFrame.currentQueryIndex=t}function vk(t){let n=t[Z];return n.type===2?n.declTNode:n.type===1?t[Qt]:null}function hg(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=vk(o),r===null||(o=o[Fo],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=Ce.lFrame=KC();return i.currentTNode=n,i.lView=t,!0}function Fu(t){let n=KC(),e=t[Z];Ce.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function KC(){let t=Ce.lFrame,n=t===null?null:t.child;return n===null?XC(t):n}function XC(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function JC(){let t=Ce.lFrame;return Ce.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var mg=JC;function Lu(){let t=JC();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function eD(t){return(Ce.lFrame.contextLView=LC(t,Ce.lFrame.contextLView))[vt]}function Ti(){return Ce.lFrame.selectedIndex}function $r(t){Ce.lFrame.selectedIndex=t}function Bl(){let t=Ce.lFrame;return Ba(t.tView,t.selectedIndex)}function Ai(){Ce.lFrame.currentNamespace=Xm}function Vu(){return Ce.lFrame.currentNamespace}var tD=!0;function ju(){return tD}function tr(t){tD=t}function Tm(t,n=null,e=null,i){let r=gg(t,n,e,i);return r.resolveInjectorInitializers(),r}function gg(t,n=null,e=null,i,r=new Set){let o=[e||Ht,MC(t)],a;return new No(o,n||Fa(),a||null,r)}var J=class t{static THROW_IF_NOT_FOUND=Ro;static NULL=new Il;static create(n,e){if(Array.isArray(n))return Tm({name:""},e,n,"");{let i=n.name??"";return Tm({name:i},n.parent,n.providers,i)}}static \u0275prov=w({token:t,providedIn:"any",factory:()=>ne(Gm)});static __NG_ELEMENT_ID__=-1},j=new y(""),Kt=(()=>{class t{static __NG_ELEMENT_ID__=_k;static __NG_ENV_ID__=e=>e}return t})(),gu=class extends Kt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Ur(this._lView)}onDestroy(n){let e=this._lView;return rg(e,n),()=>VC(e,n)}};function _k(){return new gu(re())}var nD=!1,iD=new y(""),Ri=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new mt(!1);debugTaskTracker=d(iD,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new he(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Am=class extends S{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,kC()&&(this.destroyRef=d(Kt,{optional:!0})??void 0,this.pendingTasks=d(Ri,{optional:!0})??void 0)}emit(n){let e=ie(null);try{super.next(n)}finally{ie(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof le&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},$=Am;function vu(...t){}function vg(t){let n,e;function i(){t=vu;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function rD(t){return queueMicrotask(()=>t()),()=>{t=vu}}var _g="isAngularZone",Tl=_g+"_ID",yk=0,K=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new $(!1);onMicrotaskEmpty=new $(!1);onStable=new $(!1);onError=new $(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=nD}=n;if(typeof Zone>"u")throw new k(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,Dk(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(_g)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new k(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new k(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,bk,vu,vu);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},bk={};function yg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function Ck(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){vg(()=>{t.callbackScheduled=!1,Rm(t),t.isCheckStableRunning=!0,yg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Rm(t)}function Dk(t){let n=()=>{Ck(t)},e=yk++;t._inner=t._inner.fork({name:"angular",properties:{[_g]:!0,[Tl]:e,[Tl+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(wk(l))return i.invokeTask(o,a,s,l);try{return vC(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),_C(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return vC(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!Ek(l)&&n(),_C(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,Rm(t),yg(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function Rm(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function vC(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function _C(t){t._nesting--,yg(t)}var Al=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new $;onMicrotaskEmpty=new $;onStable=new $;onError=new $;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function wk(t){return oD(t,"__ignore_ng_zone__")}function Ek(t){return oD(t,"__scheduler_tick__")}function oD(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var qt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},vn=new y("",{factory:()=>{let t=d(K),n=d(ze),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(qt),e.handleError(i))})}}}),aD={provide:Ei,useValue:()=>{let t=d(qt,{optional:!0})},multi:!0},xk=new y("",{factory:()=>{let t=d(j).defaultView;if(!t)return;let n=d(vn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(Kt).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function bg(){return xi([IC(()=>{d(xk)})])}function I(t,n){let[e,i,r]=nm(t,n?.equal),o=e,a=o[ht];return o.set=i,o.update=r,o.asReadonly=Bu.bind(o),o}function Bu(){let t=this[ht];if(t.readonlyFn===void 0){let n=()=>this();n[ht]=t,t.readonlyFn=n}return t.readonlyFn}var Ua=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Sk}return t})();function Sk(){return new Ua(re(),Mt())}var Qn=class{},Hl=new y("",{factory:()=>!0});var Cg=new y(""),za=(()=>{class t{internalPendingTasks=d(Ri);scheduler=d(Qn);errorHandler=d(vn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Hu=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>new km})}return t})(),km=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},_u=class{[ht];constructor(n){this[ht]=n}destroy(){this[ht].destroy()}};function ti(t,n){let e=n?.injector??d(J),i=n?.manualCleanup!==!0?e.get(Kt):null,r,o=e.get(Ua,null,{optional:!0}),a=e.get(Qn);return o!==null?(r=Tk(o.view,a,t),i instanceof gu&&i._lView===o.view&&(i=null)):r=Ak(t,e.get(Hu),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new _u(r)}var sD=se(D({},im),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Ml(!1);try{rm(this)}finally{Ml(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ie(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ie(t)}}}),Ik=se(D({},sD),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Ar(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Mk=se(D({},sD),{consumerMarkedDirty(){this.view[de]|=8192,Ho(this.view),this.notifier.notify(13)},destroy(){if(Ar(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Vr]?.delete(this)}});function Tk(t,n,e){let i=Object.create(Mk);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=lD(i,e),t[Vr]??=new Set,t[Vr].add(i),i.consumerMarkedDirty(i),i}function Ak(t,n,e){let i=Object.create(Ik);return i.fn=lD(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function lD(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}var Uu={JSACTION:"jsaction"};function ec(t){return{toString:t}.toString()}function Vk(t){return typeof t=="function"}function ew(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var ef=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Ae=(()=>{let t=()=>tw;return t.ngInherit=!0,t})();function tw(t){return t.type.prototype.ngOnChanges&&(t.setInput=Bk),jk}function jk(){let t=iw(this),n=t?.current;if(n){let e=t.previous;if(e===Kn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function Bk(t,n,e,i,r){let o=this.declaredInputs[i],a=iw(t)||Hk(t,{previous:Kn,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new ef(c&&c.currentValue,e,l===Kn),ew(t,n,r,e)}var nw="__ngSimpleChanges__";function iw(t){return t[nw]||null}function Hk(t,n){return t[nw]=n}var cD=[];var Fe=function(t,n=null,e){for(let i=0;i<cD.length;i++){let r=cD[i];r(t,n,e)}},Te=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Te||{});function Uk(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=tw(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function rw(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function qu(t,n,e){ow(t,n,3,e)}function Yu(t,n,e,i){(t[de]&3)===e&&ow(t,n,e,i)}function Dg(t,n){let e=t[de];(e&3)===n&&(e&=16383,e+=1,t[de]=e)}function ow(t,n,e,i){let r=i!==void 0?t[Lo]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[Lo]+=65536),(s<o||o==-1)&&(zk(t,e,n,l),t[Lo]=(t[Lo]&4294901760)+l+2),l++}function dD(t,n){Fe(Te.LifecycleHookStart,t,n);let e=ie(null);try{n.call(t)}finally{ie(e),Fe(Te.LifecycleHookEnd,t,n)}}function zk(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[de]>>14<t[Lo]>>16&&(t[de]&3)===n&&(t[de]+=16384,dD(s,o)):dD(s,o)}var Ga=-1,$o=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function $k(t){return(t.flags&8)!==0}function Gk(t){return(t.flags&16)!==0}function Wk(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];qk(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function aw(t){return t===3||t===4||t===6}function qk(t){return t.charCodeAt(0)===64}function Ya(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?uD(t,e,r,null,n[++i]):uD(t,e,r,null,null))}}return t}function uD(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function sw(t){return t!==Ga}function tf(t){return t&32767}function Yk(t){return t>>16}function nf(t,n){let e=Yk(t),i=n;for(;e>0;)i=i[Fo],e--;return i}var Fg=!0;function rf(t){let n=Fg;return Fg=t,n}var Qk=256,lw=Qk-1,cw=5,Zk=0,ki={};function Kk(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Po)&&(i=e[Po]),i==null&&(i=e[Po]=Zk++);let r=i&lw,o=1<<r;n.data[t+(r>>cw)]|=o}function of(t,n){let e=dw(t,n);if(e!==-1)return e;let i=n[Z];i.firstCreatePass&&(t.injectorIndex=n.length,wg(i.data,t),wg(n,null),wg(i.blueprint,null));let r=Dv(t,n),o=t.injectorIndex;if(sw(r)){let a=tf(r),s=nf(r,n),l=s[Z].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function wg(t,n){t.push(0,0,0,0,0,0,0,0,n)}function dw(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Dv(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=mw(r),i===null)return Ga;if(e++,r=r[Fo],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ga}function Lg(t,n,e){Kk(t,n,e)}function Xk(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(aw(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function uw(t,n,e){if(e&8||t!==void 0)return t;wu(n,"NodeInjector")}function fw(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[fn],o=Wt(void 0);try{return r?r.get(n,i,e&8):zm(n,i,e&8)}finally{Wt(o)}}return uw(i,n,e)}function pw(t,n,e,i=0,r){if(t!==null){if(n[de]&2048&&!(i&2)){let a=nO(t,n,e,i,ki);if(a!==ki)return a}let o=hw(t,n,e,i,ki);if(o!==ki)return o}return fw(n,e,i,r)}function hw(t,n,e,i,r){let o=eO(e);if(typeof o=="function"){if(!hg(n,t,i))return i&1?uw(r,e,i):fw(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))wu(e);else return a}finally{mg()}}else if(typeof o=="number"){let a=null,s=dw(t,n),l=Ga,c=i&1?n[Nt][Qt]:null;for((s===-1||i&4)&&(l=s===-1?Dv(t,n):n[s+8],l===Ga||!pD(i,!1)?s=-1:(a=n[Z],s=tf(l),n=nf(l,n)));s!==-1;){let u=n[Z];if(fD(o,s,u.data)){let f=Jk(s,n,e,a,i,c);if(f!==ki)return f}l=n[s+8],l!==Ga&&pD(i,n[Z].data[s+8]===c)&&fD(o,s,n)?(a=u,s=tf(l),n=nf(l,n)):s=-1}}return r}function Jk(t,n,e,i,r,o){let a=n[Z],s=a.data[t+8],l=i==null?Mi(s)&&Fg:i!=a&&(s.type&3)!==0,c=r&1&&o===s,u=Qu(s,a,e,l,c);return u!==null?Wl(n,a,u,s,r):ki}function Qu(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?s:s+u,p=r?s+u:c;for(let h=f;h<p;h++){let _=a[h];if(h<l&&e===_||h>=l&&_.type===e)return h}if(r){let h=a[l];if(h&&ei(h)&&h.type===e)return l}return null}function Wl(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof $o){let s=o;if(s.resolving)throw Um("");let l=rf(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],u,f=s.injectImpl?Wt(s.injectImpl):null,p=hg(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&Uk(e,a[e],n)}finally{f!==null&&Wt(f),rf(l),s.resolving=!1,mg()}}return o}function eO(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Po)?t[Po]:void 0;return typeof n=="number"?n>=0?n&lw:tO:n}function fD(t,n,e){let i=1<<t;return!!(e[n+(t>>cw)]&i)}function pD(t,n){return!(t&2)&&!(t&1&&n)}var zo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return pw(this._tNode,this._lView,n,ko(i),e)}};function tO(){return new zo(Mt(),re())}function Pe(t){return ec(()=>{let n=t.prototype.constructor,e=n[Sl]||Vg(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Sl]||Vg(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Vg(t){return Om(t)?()=>{let n=Vg(It(t));return n&&n()}:Lr(t)}function nO(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[de]&2048&&!Bo(a);){let s=hw(o,a,e,i|2,ki);if(s!==ki)return s;let l=o.parent;if(!l){let c=a[Zm];if(c){let u=c.get(e,ki,i&-5);if(u!==ki)return u}l=mw(a),a=a[Fo]}o=l}return r}function mw(t){let n=t[Z],e=n.type;return e===2?n.declTNode:e===1?t[Qt]:null}function tc(t){return Xk(Mt(),t)}function iO(){return Xa(Mt(),re())}function Xa(t,n){return new V(An(t,n))}var V=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=iO}return t})();function gw(t){return t instanceof V?t.nativeElement:t}function rO(){return this._results[Symbol.iterator]()}var Ut=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new S}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=EC(n);(this._changesDetected=!wC(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=rO},vw="ngSkipHydration",oO="ngskiphydration";function _w(t){let n=t.mergedAttrs;if(n===null)return!1;for(let e=0;e<n.length;e+=2){let i=n[e];if(typeof i=="number")return!1;if(typeof i=="string"&&i.toLowerCase()===oO)return!0}return!1}function yw(t){return t.hasAttribute(vw)}function af(t){return(t.flags&128)===128}function bw(t){if(af(t))return!0;let n=t.parent;for(;n;){if(af(t)||_w(n))return!0;n=n.parent}return!1}var wv=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(wv||{}),Cw=new Map,aO=0;function sO(){return aO++}function lO(t){Cw.set(t[er],t)}function jg(t){Cw.delete(t[er])}var hD="__ngContext__";function Qa(t,n){Tn(n)?(t[hD]=n[er],lO(n)):t[hD]=n}function Dw(t){return Ew(t[Va])}function ww(t){return Ew(t[Mn])}function Ew(t){for(;t!==null&&!hn(t);)t=t[Mn];return t}var Bg;function Ev(t){Bg=t}function xw(){if(Bg!==void 0)return Bg;if(typeof document<"u")return document;throw new k(210,!1)}var kn=new y("",{factory:()=>cO}),cO="ng";var yf=new y(""),ir=new y("",{providedIn:"platform",factory:()=>"unknown"}),nc=new y(""),Yo=new y("",{factory:()=>d(j).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ja=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>{let e=new t;return e.store=dO(d(j),d(kn)),e}});store={};onSerializeCallbacks={};get(e,i){return this.store[e]!==void 0?this.store[e]:i}set(e,i){this.store[e]=i}remove(e){delete this.store[e]}hasKey(e){return this.store.hasOwnProperty(e)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(e,i){this.onSerializeCallbacks[e]=i}toJson(){for(let e in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(e))try{this.store[e]=this.onSerializeCallbacks[e]()}catch(i){console.warn("Exception in onSerialize callback: ",i)}return JSON.stringify(this.store).replace(/</g,"\\u003C").replace(/\//g,"\\u002F")}}return t})();function dO(t,n){let e=t.getElementById(n+"-state");if(e?.textContent)try{return JSON.parse(e.textContent)}catch(i){console.warn("Exception while restoring TransferState for app "+n,i)}return{}}var Sw="h",Iw="b",uO="f",fO="n",Mw="e",Tw="t",bf="c",xv="x",ql="r",Aw="i",Rw="n",Sv="d";var kw="di",Ow="s",Nw="p";var es=new y(""),Pw=!1,Iv=new y("",{factory:()=>Pw});var Mv=new y(""),Fw=!1,Lw=new y("",{factory:()=>[]}),Vw=new y(""),Tv=new y("",{factory:()=>new Map});var ic="ngb";var jw=(t,n,e)=>{let i=t,r=i.__jsaction_fns??new Map,o=r.get(n)??[];o.push(e),r.set(n,o),i.__jsaction_fns=r},Bw=(t,n)=>{let e=t,i=e.getAttribute(ic)??"",r=n.get(i)??new Set;r.has(e)||r.add(e),n.set(i,r)};var Hw=t=>{t.removeAttribute(Uu.JSACTION),t.removeAttribute(ic),t.__jsaction_fns=void 0},Uw=new y("",{factory:()=>({})}),Hg=new WeakMap;function pO(t,n){if(t==null||typeof t!="object")return;let e=Hg.get(t);e||(e=new WeakSet,Hg.set(t,e)),e.add(n)}function Av(t,n){let e=n?.__jsaction_fns?.get(t.type);if(!(!e||!n?.isConnected)&&!(n&&Hg.get(t)?.has(n)))for(let i of e)i(t)}var Ug=new Map;function zw(t,n){return Ug.set(t,n),()=>Ug.delete(t)}var mD=!1,$w=(t,n,e,i)=>{};function hO(t,n,e,i){$w(t,n,e,i)}function Gw(){mD||($w=(t,n,e,i)=>{let r=t[fn].get(kn);Ug.get(r)?.(n,e,i)},mD=!0)}var Cf=new y("");function rc(t){return(t.flags&32)===32}var mO="__nghData__",Rv=mO,gO="__nghDeferData__",Ww=gO;var Zu="ngh",qw="nghm",Yw=()=>null;function vO(t,n,e=!1){let i=t.getAttribute(Zu);if(i==null)return null;let[r,o]=i.split("|");if(i=e?o:r,!i)return null;let a=o?`|${o}`:"",s=e?r:a,l={};if(i!==""){let u=n.get(Ja,null,{optional:!0});u!==null&&(l=u.get(Rv,[])[Number(i)])}let c={data:l,firstChild:t.firstChild??null};return e&&(c.firstChild=t,Df(c,0,t.nextSibling)),s?t.setAttribute(Zu,s):t.removeAttribute(Zu),c}function Qw(){Yw=vO}function Zw(t,n,e=!1){return Yw(t,n,e)}function Kw(t){let n=t._lView;return n[Z].type===2?null:(Bo(n)&&(n=n[Oe]),n)}function _O(t){return t.textContent?.replace(/\s/gm,"")}function yO(t){let n=xw(),e=n.createNodeIterator(t,NodeFilter.SHOW_COMMENT,{acceptNode(o){let a=_O(o);return a==="ngetn"||a==="ngtns"?NodeFilter.FILTER_ACCEPT:NodeFilter.FILTER_REJECT}}),i,r=[];for(;i=e.nextNode();)r.push(i);for(let o of r)o.textContent==="ngetn"?o.replaceWith(n.createTextNode("")):o.remove()}function Df(t,n,e){t.segmentHeads??={},t.segmentHeads[n]=e}function zg(t,n){return t.segmentHeads?.[n]??null}function Xw(t){return t.get(Vw,!1,{optional:!0})}function bO(t,n){let e=t.data,i=e[Mw]?.[n]??null;return i===null&&e[bf]?.[n]&&(i=kv(t,n)),i}function Jw(t,n){return t.data[bf]?.[n]??null}function kv(t,n){let e=Jw(t,n)??[],i=0;for(let r of e)i+=r[ql]*(r[xv]??1);return i}function CO(t){if(typeof t.disconnectedNodes>"u"){let n=t.data[Sv];t.disconnectedNodes=n?new Set(n):null}return t.disconnectedNodes}function eE(t,n){if(typeof t.disconnectedNodes>"u"){let e=t.data[Sv];t.disconnectedNodes=e?new Set(e):null}return!!CO(t)?.has(n)}function wf(t,n){let e=t[Zt];return e!==null&&!ku()&&!rc(n)&&!eE(e,n.index-Oe)}function DO(t,n){let e=n.get(Cf),r=n.get(Ja).get(Ww,{}),o=!1,a=t,s=null,l=[];for(;!o&&a;){o=e.has(a);let c=e.hydrating.get(a);if(s===null&&c!=null){s=c.promise;break}l.unshift(a),a=r[a][Nw]}return{parentBlockPromise:s,hydrationQueue:l}}function Eg(t){return!!t&&t.nodeType===Node.COMMENT_NODE&&t.textContent?.trim()===qw}function gD(t){for(;t&&t.nodeType===Node.TEXT_NODE;)t=t.previousSibling;return t}function tE(t){for(let i of t.body.childNodes)if(Eg(i))return;let n=gD(t.body.previousSibling);if(Eg(n))return;let e=gD(t.head.lastChild);if(!Eg(e))throw new k(-507,!1)}function nE(t,n){let e=t.contentQueries;if(e!==null){let i=ie(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];jl(o),s.contentQueries(2,n[a],a)}}}finally{ie(i)}}}function $g(t,n,e){jl(0);let i=ie(null);try{n(t,e)}finally{ie(i)}}function Ov(t,n,e){if(Km(n)){let i=ie(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{ie(i)}}}var ri=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(ri||{});var zu;function wO(){if(zu===void 0&&(zu=null,In.trustedTypes))try{zu=In.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return zu}function Ef(t){return wO()?.createHTML(t)||t}var $u;function EO(){if($u===void 0&&($u=null,In.trustedTypes))try{$u=In.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return $u}function vD(t){return EO()?.createScriptURL(t)||t}var nr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${yu})`}},Gg=class extends nr{getTypeName(){return"HTML"}},Wg=class extends nr{getTypeName(){return"Style"}},qg=class extends nr{getTypeName(){return"Script"}},Yg=class extends nr{getTypeName(){return"URL"}},Qg=class extends nr{getTypeName(){return"ResourceURL"}};function oi(t){return t instanceof nr?t.changingThisBreaksApplicationSecurity:t}function rr(t,n){let e=iE(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${yu})`)}return e===n}function iE(t){return t instanceof nr&&t.getTypeName()||null}function Nv(t){return new Gg(t)}function Pv(t){return new Wg(t)}function Fv(t){return new qg(t)}function Lv(t){return new Yg(t)}function Vv(t){return new Qg(t)}function xO(t){let n=new Kg(t);return SO()?new Zg(n):n}var Zg=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Ef(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Kg=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Ef(n),e}};function SO(){try{return!!new window.DOMParser().parseFromString(Ef(""),"text/html")}catch{return!1}}var IO=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function oc(t){return t=String(t),t.match(IO)?t:"unsafe:"+t}function or(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function ac(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var rE=or("area,br,col,hr,img,wbr"),oE=or("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),aE=or("rp,rt"),MO=ac(aE,oE),TO=ac(oE,or("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),AO=ac(aE,or("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),_D=ac(rE,TO,AO,MO),sE=or("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),RO=or("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),kO=or("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),OO=ac(sE,RO,kO),NO=or("script,style,template"),Xg=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=LO(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=FO(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=yD(n).toLowerCase();if(!_D.hasOwnProperty(e))return this.sanitizedSomething=!0,!NO.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!OO.hasOwnProperty(s)){this.sanitizedSomething=!0;continue}let l=o.value;sE[s]&&(l=oc(l)),this.buf.push(" ",a,'="',bD(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=yD(n).toLowerCase();_D.hasOwnProperty(e)&&!rE.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(bD(n))}};function PO(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function FO(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw lE(n);return n}function LO(t){let n=t.firstChild;if(n&&PO(t,n))throw lE(n);return n}function yD(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function lE(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var VO=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,jO=/([^\#-~ |!])/g;function bD(t){return t.replace(/&/g,"&amp;").replace(VO,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(jO,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Gu;function jv(t,n){let e=null;try{Gu=Gu||xO(t);let i=n?String(n):"";e=Gu.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Gu.getInertBodyElement(i)}while(i!==o);let s=new Xg().sanitizeChildren(CD(e)||e);return Ef(s)}finally{if(e){let i=CD(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function CD(t){return"content"in t&&BO(t)?t.content:null}function BO(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var HO=/^>|^->|<!--|-->|--!>|<!-$/g,UO=/(<|>)/g,zO="\u200B$1\u200B";function $O(t){return t.replace(HO,n=>n.replace(UO,zO))}function cE(t,n){return t.createText(n)}function GO(t,n,e){t.setValue(n,e)}function dE(t,n){return t.createComment($O(n))}function Bv(t,n,e){return t.createElement(n,e)}function sf(t,n,e,i,r){t.insertBefore(n,e,i,r)}function uE(t,n,e){t.appendChild(n,e)}function DD(t,n,e,i,r){i!==null?sf(t,n,e,i,r):uE(t,n,e)}function Hv(t,n,e,i){t.removeChild(null,n,e,i)}function fE(t){t.textContent=""}function WO(t,n,e){t.setAttribute(n,"style",e)}function qO(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function pE(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&Wk(t,n,i),r!==null&&qO(t,n,r),o!==null&&WO(t,n,o)}var Tt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Tt||{});function hE(t){let n=gE();return n?n.sanitize(Tt.URL,t)||"":rr(t,"URL")?oi(t):oc(Na(t))}function mE(t){let n=gE();if(n)return vD(n.sanitize(Tt.RESOURCE_URL,t)||"");if(rr(t,"ResourceURL"))return vD(oi(t));throw new k(904,!1)}var YO={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function QO(t,n){return YO[t.toLowerCase()]?.[n.toLowerCase()]===!0?mE:hE}function Uv(t,n,e){return QO(n,e)(t)}function gE(){let t=re();return t&&t[Xn].sanitizer}function vE(t){return t.ownerDocument.body}function _E(t){return t instanceof Function?t():t}function ZO(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var yE="ng-template";function KO(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&ZO(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(zv(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function zv(t){return t.type===4&&t.value!==yE}function XO(t,n,e){let i=t.type===4&&!e?yE:t.value;return n===i}function JO(t,n,e){let i=4,r=t.attrs,o=r!==null?nN(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!ni(i)&&!ni(l))return!1;if(a&&ni(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!XO(t,l,e)||l===""&&n.length===1){if(ni(i))return!1;a=!0}}else if(i&8){if(r===null||!KO(t,r,l,e)){if(ni(i))return!1;a=!0}}else{let c=n[++s],u=eN(l,r,zv(t),e);if(u===-1){if(ni(i))return!1;a=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(ni(i))return!1;a=!0}}}}return ni(i)||a}function ni(t){return(t&1)===0}function eN(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return iN(n,t)}function bE(t,n,e=!1){for(let i=0;i<n.length;i++)if(JO(t,n[i],e))return!0;return!1}function tN(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function nN(t){for(let n=0;n<t.length;n++){let e=t[n];if(aw(e))return n}return t.length}function iN(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function rN(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function wD(t,n){return t?":not("+n.trim()+")":n}function oN(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!ni(a)&&(n+=wD(o,r),r=""),i=a,o=o||!ni(i);e++}return r!==""&&(n+=wD(o,r)),n}function aN(t){return t.map(oN).join(",")}function sN(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!ni(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Xt={};function $v(t,n,e,i,r,o,a,s,l,c,u){let f=Oe+i,p=f+r,h=lN(f,p),_=typeof c=="function"?c():c;return h[Z]={type:t,blueprint:h,template:e,queries:null,viewQuery:s,declTNode:n,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:_,incompleteFirstPass:!1,ssrId:u}}function lN(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Xt);return e}function cN(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=$v(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Gv(t,n,e,i,r,o,a,s,l,c,u){let f=n.blueprint.slice();return f[Yt]=r,f[de]=i|4|128|8|64|1024,(c!==null||t&&t[de]&2048)&&(f[de]|=2048),ng(f),f[gt]=f[Fo]=t,f[vt]=e,f[Xn]=a||t&&t[Xn],f[ke]=s||t&&t[ke],f[fn]=l||t&&t[fn]||null,f[Qt]=o,f[er]=sO(),f[Zt]=u,f[Zm]=c,f[Nt]=n.type==2?t[Nt]:f,f}function dN(t,n,e){let i=An(n,t),r=cN(e),o=t[Xn].rendererFactory,a=Wv(t,Gv(t,r,null,CE(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function CE(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function DE(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Wv(t,n){return t[Va]?t[Qm][Mn]=n:t[Va]=n,t[Qm]=n,n}function g(t=1){wE(Ye(),re(),Ti()+t,!1)}function wE(t,n,e,i){if(!i)if((n[de]&3)===3){let o=t.preOrderCheckHooks;o!==null&&qu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Yu(n,o,0,e)}$r(e)}var xf=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(xf||{});function Jg(t,n,e,i){let r=ie(null);try{let[o,a,s]=t.inputs[e],l=null;(a&xf.SignalBased)!==0&&(l=n[o][ht]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):ew(n,l,o,i)}finally{ie(r)}}var Oi=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Oi||{}),uN;function qv(t,n){return uN(t,n)}var iY=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var ev=new WeakMap,zl=new WeakSet;function fN(t,n){let e=ev.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let a=e[o],s=a.parentNode;a===n?(e.splice(o,1),zl.add(a),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&a===r||s&&i&&s!==i)&&(e.splice(o,1),a.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),a.parentNode?.removeChild(a))}}function pN(t,n){let e=ev.get(t);e?e.includes(n)||e.push(n):ev.set(t,[n])}var Go=new Set,Sf=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Sf||{}),ai=new y(""),ED=new Set;function On(t){ED.has(t)||(ED.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var If=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Yv=[0,1,2,3],Qv=(()=>{class t{ngZone=d(K);scheduler=d(Qn);errorHandler=d(qt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(ai,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Fe(Te.AfterRenderHooksStart),this.executing=!0;for(let i of Yv)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Fe(Te.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Vo]??=[]).push(e),Ho(i),i[de]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Sf.AFTER_NEXT_RENDER,e):e()}static \u0275prov=w({token:t,providedIn:"root",factory:()=>new t})}return t})(),Yl=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Vo];n&&(this.view[Vo]=n.filter(e=>e!==this))}};function lt(t,n){let e=n?.injector??d(J);return On("NgAfterNextRender"),mN(t,e,n,!0)}function hN(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function mN(t,n,e,i){let r=n.get(If);r.impl??=n.get(Qv);let o=n.get(ai,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(Kt):null,s=n.get(Ua,null,{optional:!0}),l=new Yl(r.impl,hN(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var EE=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(ze)})});function xE(t,n,e){let i=t.get(EE);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function gN(t,n){let e=t.get(EE);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function vN(t,n){for(let[e,i]of n)xE(t,i.animateFns)}function xD(t,n,e,i){let r=t?.[Hr]?.enter;n!==null&&r&&r.has(e.index)&&vN(i,r)}function $a(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;hn(r)?l=r:Tn(r)&&(c=!0,r=r[Yt]);let u=mn(r);t===0&&i!==null?(xD(s,i,o,e),a==null?uE(n,i,u):sf(n,i,u,a||null,!0)):t===1&&i!==null?(xD(s,i,o,e),sf(n,i,u,a||null,!0),fN(o,u)):t===2?(s?.[Hr]?.leave?.has(o.index)&&pN(o,u),zl.delete(u),SD(s,o,e,f=>{if(zl.has(u)){zl.delete(u);return}Hv(n,u,c,f)})):t===3&&(zl.delete(u),SD(s,o,e,()=>{n.destroyNode(u)})),l!=null&&MN(n,t,e,l,o,i,a)}}function _N(t,n){SE(t,n),n[Yt]=null,n[Qt]=null}function yN(t,n,e,i,r,o){i[Yt]=r,i[Qt]=n,Tf(t,i,e,1,r,o)}function SE(t,n){n[Xn].changeDetectionScheduler?.notify(9),Tf(t,n,n[ke],2,null,null)}function bN(t){let n=t[Va];if(!n)return xg(t[Z],t);for(;n;){let e=null;if(Tn(n))e=n[Va];else{let i=n[st];i&&(e=i)}if(!e){for(;n&&!n[Mn]&&n!==t;)Tn(n)&&xg(n[Z],n),n=n[gt];n===null&&(n=t),Tn(n)&&xg(n[Z],n),e=n&&n[Mn]}n=e}}function Zv(t,n){let e=t[jo],i=e.indexOf(n);e.splice(i,1)}function Mf(t,n){if(Ur(n))return;let e=n[ke];e.destroyNode&&Tf(t,n,e,3,null,null),bN(n)}function xg(t,n){if(Ur(n))return;let e=ie(null);try{n[de]&=-129,n[de]|=256,n[pn]&&Ar(n[pn]),wN(t,n),DN(t,n),n[Z].type===1&&n[ke].destroy();let i=n[Br];if(i!==null&&hn(n[gt])){i!==n[gt]&&Zv(i,n);let r=n[Si];r!==null&&r.detachView(t)}jg(n)}finally{ie(e)}}function SD(t,n,e,i){let r=t?.[Hr];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Go.add(t[er]),xE(e,()=>{if(r.leave&&r.leave.has(n.index)){let a=r.leave.get(n.index),s=[];if(a){for(let l=0;l<a.animateFns.length;l++){let c=a.animateFns[l],{promise:u}=c();s.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(s),CN(t,i)}else t&&Go.delete(t[er]),i(!1)},r)}function CN(t,n){let e=t[Hr]?.running;if(e){e.then(()=>{t[Hr].running=void 0,Go.delete(t[er]),n(!0)});return}n(!1)}function DN(t,n){let e=t.cleanup,i=n[La];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[La]=null);let r=n[Ji];if(r!==null){n[Ji]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[Vr];if(o!==null){n[Vr]=null;for(let a of o)a.destroy()}}function wN(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof $o)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];Fe(Te.LifecycleHookStart,s,l);try{l.call(s)}finally{Fe(Te.LifecycleHookEnd,s,l)}}else{Fe(Te.LifecycleHookStart,r,o);try{o.call(r)}finally{Fe(Te.LifecycleHookEnd,r,o)}}}}}function IE(t,n,e){return EN(t,n.parent,e)}function EN(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Yt];if(Mi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===ri.None||r===ri.Emulated)return null}return An(i,e)}function ME(t,n,e){return SN(t,n,e)}function xN(t,n,e){return t.type&40?An(t,e):null}var SN=xN,ID;function Kv(t,n,e,i){let r=IE(t,i,n),o=n[ke],a=i.parent||n[Qt],s=ME(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)DD(o,r,e[l],s,!1);else DD(o,r,e,s,!1);ID!==void 0&&ID(o,i,n,e,r)}function $l(t,n){if(n!==null){let e=n.type;if(e&3)return An(n,t);if(e&4)return tv(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return $l(t,i);{let r=t[n.index];return hn(r)?tv(-1,r):mn(r)}}else{if(e&128)return $l(t,n.next);if(e&32)return qv(n,t)()||mn(t[n.index]);{let i=TE(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=jr(t[Nt]);return $l(r,i)}else return $l(t,n.next)}}}return null}function TE(t,n){if(n!==null){let i=t[Nt][Qt],r=n.projection;return i.projection[r]}return null}function tv(t,n){let e=st+t+1;if(e<n.length){let i=n[e],r=i[Z].firstChild;if(r!==null)return $l(i,r)}return n[Ii]}function Xv(t,n,e,i,r,o,a){for(;e!=null;){let s=i[fn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Qa(mn(l),i),e.flags|=2),!rc(e))if(c&8)Xv(t,n,e.child,i,r,o,!1),$a(n,t,s,r,l,e,o,i);else if(c&32){let u=qv(e,i),f;for(;f=u();)$a(n,t,s,r,f,e,o,i);$a(n,t,s,r,l,e,o,i)}else c&16?AE(t,n,i,e,r,o):$a(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function Tf(t,n,e,i,r,o){Xv(e,i,t.firstChild,n,r,o,!1)}function IN(t,n,e){let i=n[ke],r=IE(t,e,n),o=e.parent||n[Qt],a=ME(o,e,n);AE(i,0,n,e,r,a)}function AE(t,n,e,i,r,o){let a=e[Nt],l=a[Qt].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];$a(n,t,e[fn],r,u,i,o,e)}else{let c=l,u=a[gt];af(i)&&(c.flags|=128),Xv(t,n,c,u,r,o,!0)}}function MN(t,n,e,i,r,o,a){let s=i[Ii],l=mn(i);s!==l&&$a(n,t,e,o,s,r,a);for(let c=st;c<i.length;c++){let u=i[c];Tf(u[Z],u,t,n,o,s)}}function TN(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Oi.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Oi.Important),t.setStyle(e,i,r,o))}}function RE(t,n,e,i,r){let o=Ti(),a=i&2;try{$r(-1),a&&n.length>Oe&&wE(t,n,Oe,!1);let s=a?Te.TemplateUpdateStart:Te.TemplateCreateStart;Fe(s,r,e),e(i,r)}finally{$r(o);let s=a?Te.TemplateUpdateEnd:Te.TemplateCreateEnd;Fe(s,r,e)}}function Af(t,n,e){PN(t,n,e),(e.flags&64)===64&&FN(t,n,e)}function sc(t,n,e=An){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function AN(t,n,e,i){let o=i.get(Iv,Pw)||e===ri.ShadowDom||e===ri.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);if(a.tagName.toLowerCase()==="script")throw new k(905,!1);return RN(a),a}function RN(t){kE(t)}var kE=()=>null;function kN(t){yw(t)?fE(t):yO(t)}function OE(){kE=kN}function ON(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function NE(t,n,e,i,r,o){let a=n[Z];if(i_(t,a,n,e,i)){Mi(t)&&NN(n,t.index);return}t.type&3&&(e=ON(e)),PE(t,n,e,i,r,o)}function PE(t,n,e,i,r,o){if(t.type&3){let a=An(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function NN(t,n){let e=Rn(n,t);e[de]&16||(e[de]|=64)}function PN(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Mi(e)&&dN(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||of(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=Wl(n,t,a,e);if(Qa(l,n),o!==null&&BN(n,a-i,l,s,e,o),ei(s)){let c=Rn(e.index,n);c[vt]=Wl(n,t,a,e)}}}function FN(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=QC();try{$r(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];Nu(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&LN(l,c)}}finally{$r(-1),Nu(a)}}function LN(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Jv(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];bE(n,o.selectors,!1)&&(i??=[],ei(o)?i.unshift(o):i.push(o))}return i}function VN(t,n,e,i,r,o){let a=An(t,n);jN(n[ke],a,o,t.value,e,i,r)}function jN(t,n,e,i,r,o,a){if(o==null)t.removeAttribute(n,r,e);else{let s=a==null?Na(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function BN(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];Jg(i,e,l,c)}}function e_(t,n,e,i,r){let o=Oe+e,a=n[Z],s=r(a,n,t,i,e);n[o]=s,Ha(t,!0);let l=t.type===2;return l?(pE(n[ke],s,t),(BC()===0||ja(t))&&Qa(s,n),HC()):Qa(s,n),ju()&&(!l||!rc(t))&&Kv(a,n,s,t),t}function t_(t){let n=t;return ug()?fg():(n=n.parent,Ha(n,!1)),n}function n_(t,n){let e=t[fn];if(!e)return;let i;try{i=e.get(vn,null)}catch{i=null}i?.(n)}function i_(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],u=a[l+1],f=n.data[c];Jg(f,e[c],u,r),s=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];Jg(u,c,i,r),s=!0}return s}function HN(t,n){let e=Rn(n,t),i=e[Z];UN(i,e);let r=e[Yt];r!==null&&e[Zt]===null&&(e[Zt]=Zw(r,e[fn])),Fe(Te.ComponentStart);try{r_(i,e,e[vt])}finally{Fe(Te.ComponentEnd,e[vt])}}function UN(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function r_(t,n,e){Fu(n);try{let i=t.viewQuery;i!==null&&$g(1,i,e);let r=t.template;r!==null&&RE(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Si]?.finishViewCreation(t),t.staticContentQueries&&nE(t,n),t.staticViewQueries&&$g(2,t.viewQuery,e);let o=t.components;o!==null&&zN(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[de]&=-5,Lu()}}function zN(t,n){for(let e=0;e<n.length;e++)HN(t,n[e])}function ts(t,n,e,i){let r=ie(null);try{let o=n.tView,s=t[de]&4096?4096:16,l=Gv(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Br]=c;let u=t[Si];return u!==null&&(l[Si]=u.createEmbeddedView(o)),r_(o,l,e),l}finally{ie(r)}}function Wo(t,n){return!n||n.firstChild===null||af(t)}function Ql(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(mn(o)),hn(o)&&FE(o,i);let a=e.type;if(a&8)Ql(t,n,e.child,i);else if(a&32){let s=qv(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=TE(n,e);if(Array.isArray(s))i.push(...s);else{let l=jr(n[Nt]);Ql(l[Z],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function FE(t,n){for(let e=st;e<t.length;e++){let i=t[e],r=i[Z].firstChild;r!==null&&Ql(i[Z],i,r,n)}t[Ii]!==t[Yt]&&n.push(t[Ii])}function LE(t){if(t[Vo]!==null){for(let n of t[Vo])n.impl.addSequence(n);t[Vo].length=0}}var VE=[];function $N(t){return t[pn]??GN(t)}function GN(t){let n=VE.pop()??Object.create(qN);return n.lView=t,n}function WN(t){t.lView[pn]!==t&&(t.lView=null,VE.push(t))}var qN=se(D({},Ir),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Ho(t.lView)},consumerOnSignalRead(){this.lView[pn]=this}});function YN(t){let n=t[pn]??Object.create(QN);return n.lView=t,n}var QN=se(D({},Ir),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=jr(t.lView);for(;n&&!jE(n[Z]);)n=jr(n);n&&ig(n)},consumerOnSignalRead(){this.lView[pn]=this}});function jE(t){return t.type!==2}function BE(t){if(t[Vr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Vr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[de]&8192)}}var ZN=100;function HE(t,n=0){let i=t[Xn].rendererFactory,r=!1;r||i.begin?.();try{KN(t,n)}finally{r||i.end?.()}}function KN(t,n){let e=pg();try{Ml(!0),nv(t,n);let i=0;for(;Ll(t);){if(i===ZN)throw new k(103,!1);i++,nv(t,1)}}finally{Ml(e)}}function XN(t,n,e,i){if(Ur(n))return;let r=n[de],o=!1,a=!1;Fu(n);let s=!0,l=null,c=null;o||(jE(t)?(c=$N(n),l=Zi(c)):Rd()===null?(s=!1,c=YN(n),l=Zi(c)):n[pn]&&(Ar(n[pn]),n[pn]=null));try{ng(n),WC(t.bindingStartIndex),e!==null&&RE(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let h=t.preOrderCheckHooks;h!==null&&qu(n,h,null)}else{let h=t.preOrderHooks;h!==null&&Yu(n,h,0,null),Dg(n,0)}if(a||JN(n),BE(n),UE(n,0),t.contentQueries!==null&&nE(t,n),!o)if(u){let h=t.contentCheckHooks;h!==null&&qu(n,h)}else{let h=t.contentHooks;h!==null&&Yu(n,h,1),Dg(n,1)}t1(t,n);let f=t.components;f!==null&&$E(n,f,0);let p=t.viewQuery;if(p!==null&&$g(2,p,i),!o)if(u){let h=t.viewCheckHooks;h!==null&&qu(n,h)}else{let h=t.viewHooks;h!==null&&Yu(n,h,2),Dg(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Mu]){for(let h of n[Mu])h();n[Mu]=null}o||(LE(n),n[de]&=-73)}catch(u){throw o||Ho(n),u}finally{c!==null&&(Tr(c,l),s&&WN(c)),Lu()}}function UE(t,n){for(let e=Dw(t);e!==null;e=ww(e))for(let i=st;i<e.length;i++){let r=e[i];zE(r,n)}}function JN(t){for(let n=Dw(t);n!==null;n=ww(n)){if(!(n[de]&2))continue;let e=n[jo];for(let i=0;i<e.length;i++){let r=e[i];ig(r)}}}function e1(t,n,e){Fe(Te.ComponentStart);let i=Rn(n,t);try{zE(i,e)}finally{Fe(Te.ComponentEnd,i[vt])}}function zE(t,n){Tu(t)&&nv(t,n)}function nv(t,n){let i=t[Z],r=t[de],o=t[pn],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&wa(o)),a||=!1,o&&(o.dirty=!1),t[de]&=-9217,a)XN(i,t,i.template,t[vt]);else if(r&8192){let s=ie(null);try{BE(t),UE(t,1);let l=i.components;l!==null&&$E(t,l,1),LE(t)}finally{ie(s)}}}function $E(t,n,e){for(let i=0;i<n.length;i++)e1(t,n[i],e)}function t1(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)$r(~r);else{let o=r,a=e[++i],s=e[++i];YC(a,o);let l=n[o];Fe(Te.HostBindingsUpdateStart,l);try{s(2,l)}finally{Fe(Te.HostBindingsUpdateEnd,l)}}}}finally{$r(-1)}}function Rf(t,n){let e=pg()?64:1088;for(t[Xn].changeDetectionScheduler?.notify(n);t;){t[de]|=e;let i=jr(t);if(Bo(t)&&!i)return t;t=i}return null}function GE(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function WE(t,n){let e=st+n;if(e<t.length)return t[e]}function ns(t,n,e,i=!0){let r=n[Z];if(n1(r,n,t,e),i){let a=tv(e,t),s=n[ke],l=s.parentNode(t[Ii]);l!==null&&yN(r,t[Qt],s,n,l,a)}let o=n[Zt];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function o_(t,n){let e=Zl(t,n);return e!==void 0&&Mf(e[Z],e),e}function Zl(t,n){if(t.length<=st)return;let e=st+n,i=t[e];if(i){let r=i[Br];r!==null&&r!==t&&Zv(r,i),n>0&&(t[e-1][Mn]=i[Mn]);let o=Nl(t,st+n);_N(i[Z],i);let a=o[Si];a!==null&&a.detachView(o[Z]),i[gt]=null,i[Mn]=null,i[de]&=-129}return i}function n1(t,n,e,i){let r=st+i,o=e.length;i>0&&(e[r-1][Mn]=n),i<o-st?(n[Mn]=e[r],$m(e,st+i,n)):(e.push(n),n[Mn]=null),n[gt]=e;let a=n[Br];a!==null&&e!==a&&qE(a,n);let s=n[Si];s!==null&&s.insertView(t),Au(n),n[de]|=128}function qE(t,n){let e=t[jo],i=n[gt];if(Tn(i))t[de]|=2;else{let r=i[gt][Nt];n[Nt]!==r&&(t[de]|=2)}e===null?t[jo]=[n]:e.push(n)}var Gr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[Z];return Ql(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[vt]}set context(n){this._lView[vt]=n}get destroyed(){return Ur(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[gt];if(hn(n)){let e=n[Fl],i=e?e.indexOf(this):-1;i>-1&&(Zl(n,i),Nl(e,i))}this._attachedToViewContainer=!1}Mf(this._lView[Z],this._lView)}onDestroy(n){rg(this._lView,n)}markForCheck(){Rf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[de]&=-129}reattach(){Au(this._lView),this._lView[de]|=128}detectChanges(){this._lView[de]|=1024,HE(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new k(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Bo(this._lView),e=this._lView[Br];e!==null&&!n&&Zv(e,this._lView),SE(this._lView[Z],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new k(902,!1);this._appRef=n;let e=Bo(this._lView),i=this._lView[Br];i!==null&&!e&&qE(i,this._lView),Au(this._lView)}};var Ne=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=i1;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=ts(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Gr(o)}}return t})();function i1(){return kf(Mt(),re())}function kf(t,n){return t.type&4?new Ne(n,t,Xa(t,n)):null}function is(t,n,e,i,r){let o=t.data[n];if(o===null)o=r1(t,n,e,i,r),qC()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=zC();o.injectorIndex=a===null?-1:a.injectorIndex}return Ha(o,!0),o}function r1(t,n,e,i,r){let o=dg(),a=ug(),s=a?o:o&&o.parent,l=t.data[n]=a1(t,s,e,n,i,r);return o1(t,l,o,a),l}function o1(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function a1(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return ku()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:Vu(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var s1=new RegExp(`^(\\d+)*(${Iw}|${Sw})*(.*)`);function l1(t){let n=t.match(s1),[e,i,r,o]=n,a=i?parseInt(i,10):r,s=[];for(let[l,c,u]of o.matchAll(/(f|n)(\d*)/g)){let f=parseInt(u,10)||1;s.push(c,f)}return[a,...s]}function c1(t){return!t.prev&&t.parent?.type===8}function Sg(t){return t.index-Oe}function d1(t,n){let e=t.i18nNodes;if(e)return e.get(n)}function Of(t,n,e,i){let r=Sg(i),o=d1(t,r);if(o===void 0){let a=t.data[Rw];if(a?.[r])o=f1(a[r],e);else if(n.firstChild===i)o=t.firstChild;else{let s=i.prev===null,l=i.prev??i.parent;if(c1(i)){let c=Sg(i.parent);o=zg(t,c)}else{let c=An(l,e);if(s)o=c.firstChild;else{let u=Sg(l),f=zg(t,u);if(l.type===2&&f){let h=kv(t,u)+1;o=Nf(h,f)}else o=c.nextSibling}}}}return o}function Nf(t,n){let e=n;for(let i=0;i<t;i++)e=e.nextSibling;return e}function u1(t,n){let e=t;for(let i=0;i<n.length;i+=2){let r=n[i],o=n[i+1];for(let a=0;a<o;a++)switch(r){case uO:e=e.firstChild;break;case fO:e=e.nextSibling;break}}return e}function f1(t,n){let[e,...i]=l1(t),r;if(e===Sw)r=n[Nt][Yt];else if(e===Iw)r=vE(n[Nt][Yt]);else{let o=Number(e);r=mn(n[o+Oe])}return u1(r,i)}var p1=!1;function YE(t){p1=t}function h1(t){let n=t[Zt];if(n){let{i18nNodes:e,dehydratedIcuData:i}=n;if(e&&i){let r=t[ke];for(let o of i.values())m1(r,e,o)}n.i18nNodes=void 0,n.dehydratedIcuData=void 0}}function m1(t,n,e){for(let i of e.node.cases[e.case]){let r=n.get(i.index-Oe);r&&Hv(t,r,!1)}}function Pf(t){let n=t[Jn]??[],i=t[gt][ke],r=[];for(let o of n)o.data[kw]!==void 0?r.push(o):QE(o,i);t[Jn]=r}function g1(t){let{lContainer:n}=t,e=n[Jn];if(e===null)return;let r=n[gt][ke];for(let o of e)QE(o,r)}function QE(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[ql];for(;e<r;){let o=i.nextSibling;Hv(n,i,!1),i=o,e++}}}function Ff(t){Pf(t);let n=t[Yt];Tn(n)&&lf(n);for(let e=st;e<t.length;e++)lf(t[e])}function lf(t){h1(t);let n=t[Z];for(let e=Oe;e<n.bindingStartIndex;e++)if(hn(t[e])){let i=t[e];Ff(i)}else Tn(t[e])&&lf(t[e])}function a_(t){let n=t._views;for(let e of n){let i=Kw(e);i!==null&&i[Yt]!==null&&(Tn(i)?lf(i):Ff(i))}}function v1(t,n,e,i){t!==null&&(e.cleanup(n),Ff(t.lContainer),a_(i))}function _1(t,n){let e=[];for(let i of n)for(let r=0;r<(i[xv]??1);r++){let o={data:i,firstChild:null};i[ql]>0&&(o.firstChild=t,t=Nf(i[ql],t)),e.push(o)}return[t,e]}var ZE=()=>null,KE=()=>null;function XE(){ZE=y1,KE=b1}function y1(t,n){return ex(t,n)?t[Jn].shift():(Pf(t),null)}function Kl(t,n){return ZE(t,n)}function b1(t,n,e){if(n.tView.ssrId===null)return null;let i=Kl(t,n.tView.ssrId);return e[Z].firstUpdatePass&&i===null&&C1(e,n),i}function JE(t,n,e){return KE(t,n,e)}function C1(t,n){let e=n;for(;e;){if(MD(t,e))return;if((e.flags&256)===256)break;e=e.prev}for(e=n.next;e&&(e.flags&512)===512;){if(MD(t,e))return;e=e.next}}function ex(t,n){let e=t[Jn];return!n||e===null||e.length===0?!1:e[0].data[Aw]===n}function MD(t,n){let e=n.tView?.ssrId;if(e==null)return!1;let i=t[n.index];return hn(i)&&ex(i,e)?(Pf(i),!0):!1}var tx=class{},Lf=class{},iv=class{resolveComponentFactory(n){throw new k(917,!1)}},lc=class{static NULL=new iv},wt=class{},Ge=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>D1()}return t})();function D1(){let t=re(),n=Mt(),e=Rn(n.index,t);return(Tn(e)?e:t)[ke]}var nx=(()=>{class t{static \u0275prov=w({token:t,providedIn:"root",factory:()=>null})}return t})();var Ku={},Wa=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Ku,i);return r!==Ku||e===Ku?r:this.parentInjector.get(n,e,i)}};function cf(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=bu(r,s);else if(o==2){let l=s,c=n[++a];i=bu(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ee(t,n=0){let e=re();if(e===null)return ne(t,n);let i=Mt();return pw(i,e,It(t),n)}function Vf(){let t="invalid";throw new Error(t)}function ix(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let u of a)if(u.resolveHostDirectives!==null){[s,l,c]=u.resolveHostDirectives(a);break}x1(t,n,e,s,o,l,c)}o!==null&&i!==null&&w1(e,i,o)}function w1(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new k(-301,!1);i.push(n[r],o)}}function E1(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function x1(t,n,e,i,r,o,a){let s=i.length,l=null;for(let p=0;p<s;p++){let h=i[p];l===null&&ei(h)&&(l=h,E1(t,e,p)),Lg(of(e,n),t,h.type)}R1(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let p=0;p<s;p++){let h=i[p];h.providersResolver&&h.providersResolver(h)}let c=!1,u=!1,f=DE(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let p=0;p<s;p++){let h=i[p];if(e.mergedAttrs=Ya(e.mergedAttrs,h.hostAttrs),I1(t,e,n,f,h),A1(f,h,r),a!==null&&a.has(h)){let[x,T]=a.get(h);e.directiveToIndex.set(h.type,[f,x+e.directiveStart,T+e.directiveStart])}else(o===null||!o.has(h))&&e.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(e.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(e.flags|=64);let _=h.type.prototype;!c&&(_.ngOnChanges||_.ngOnInit||_.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(_.ngOnChanges||_.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}S1(t,e,o)}function S1(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))TD(0,n,r,i),TD(1,n,r,i),RD(n,i,!1);else{let o=e.get(r);AD(0,n,o,i),AD(1,n,o,i),RD(n,i,!0)}}}function TD(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),rx(n,o)}}function AD(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),rx(n,a)}}function rx(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function RD(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||zv(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){a??=[],a.push(c[u+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function I1(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Lr(r.type,!0)),a=new $o(o,ei(r),ee,null);t.blueprint[i]=a,e[i]=a,M1(t,n,i,DE(t,e,r.hostVars,Xt),r)}function M1(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;T1(a)!=s&&a.push(s),a.push(e,i,o)}}function T1(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function A1(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;ei(n)&&(e[""]=t)}}function R1(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function s_(t,n,e,i,r,o,a,s){let l=n[Z],c=l.consts,u=gn(c,a),f=is(l,t,e,i,u);return o&&ix(l,n,f,gn(c,s),r),f.mergedAttrs=Ya(f.mergedAttrs,f.attrs),f.attrs!==null&&cf(f,f.attrs,!1),f.mergedAttrs!==null&&cf(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function l_(t,n){rw(t,n),Km(n)&&t.queries.elementEnd(n)}function k1(t,n,e,i,r,o){let a=n.consts,s=gn(a,r),l=is(n,t,e,i,s);if(l.mergedAttrs=Ya(l.mergedAttrs,l.attrs),o!=null){let c=gn(a,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&cf(l,l.attrs,!1),l.mergedAttrs!==null&&cf(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function c_(t){return ax(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function ox(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function ax(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function d_(t,n,e){return t[n]=e}function O1(t,n){return t[n]}function yn(t,n,e){if(e===Xt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function sx(t,n,e,i){let r=yn(t,n,e);return yn(t,n+1,i)||r}function N1(t,n,e,i,r){let o=sx(t,n,e,i);return yn(t,n+2,r)||o}function Xu(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&pO(r,o);let a=Mi(t)?Rn(t.index,n):n;Rf(a,5);let s=n[vt],l=kD(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=kD(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function kD(t,n,e,i){let r=ie(null);try{return Fe(Te.OutputStart,n,e),e(i)!==!1}catch(o){return n_(t,o),!1}finally{Fe(Te.OutputEnd,n,e),ie(r)}}function lx(t,n,e,i,r,o,a,s){let l=ja(t),c=!1,u=null;if(!i&&l&&(u=F1(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=a,u.__ngLastListenerFn__=a,c=!0}else{let f=An(t,e),p=i?i(f):f;hO(e,p,o,s),i||(s.__ngNativeEl__=f);let h=r.listen(p,o,s);if(!P1(o)){let _=i?x=>i(mn(x[t.index])):t.index;cx(_,n,e,o,s,h,!1)}}return c}function P1(t){return t.startsWith("animation")||t.startsWith("transition")}function F1(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[La],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function cx(t,n,e,i,r,o,a){let s=n.firstCreatePass?ag(n):null,l=og(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function OD(t,n,e,i,r,o){let a=n[e],s=n[Z],c=s.data[e].outputs[i],f=a[c].subscribe(o);cx(t.index,s,n,r,o,f,!0)}var rv=Symbol("BINDING");function dx(t){return t.debugInfo?.className||t.type.name||null}var df=class extends lc{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=Zn(n);return new Wr(e,this.ngModule)}};function L1(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&xf.SignalBased)!==0};return r&&(o.transform=r),o})}function V1(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function j1(t,n,e){let i=n instanceof ze?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Wa(e,i):e}function B1(t){let n=t.get(wt,null);if(n===null)throw new k(407,!1);let e=t.get(nx,null),i=t.get(Qn,null),r=t.get(ai,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function H1(t,n){let e=ux(t);return Bv(n,e,e==="svg"?Xm:e==="math"?NC:null)}function ux(t){return(t.selectors[0][0]||"div").toLowerCase()}var Wr=class extends Lf{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=L1(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=V1(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=aN(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){Fe(Te.DynamicComponentStart);let s=ie(null);try{let l=this.componentDef,c=j1(l,r||this.ngModule,n),u=B1(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(dx(l),()=>this.createComponentRef(u,c,e,i,o,a)):this.createComponentRef(u,c,e,i,o,a)}finally{ie(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,l=U1(r,s,a,o),c=n.rendererFactory.createRenderer(null,s),u=r?AN(c,r,s.encapsulation,e):H1(s,c),f=a?.some(ND)||o?.some(_=>typeof _!="function"&&_.bindings.some(ND)),p=Gv(null,l,null,512|CE(s),null,null,n,c,e,null,Zw(u,e,!0));p[Oe]=u,Fu(p);let h=null;try{let _=s_(Oe,p,2,"#host",()=>l.directiveRegistry,!0,0);pE(c,u,_),Qa(u,p),Af(l,p,_),Ov(l,_,p),l_(l,_),i!==void 0&&$1(_,this.ngContentSelectors,i),h=Rn(_.index,p),p[vt]=h[vt],r_(l,p,null)}catch(_){throw h!==null&&jg(h),jg(p),_}finally{Fe(Te.DynamicComponentEnd),Lu()}return new uf(this.componentType,p,!!f)}};function U1(t,n,e,i){let r=t?["ng-version","21.2.15"]:sN(n.selectors[0]),o=null,a=null,s=0;if(e)for(let u of e)s+=u[rv].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(a??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let p of f.bindings){s+=p[rv].requiredVars;let h=u+1;p.create&&(p.targetIdx=h,(o??=[]).push(p)),p.update&&(p.targetIdx=h,(a??=[]).push(p))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,p=Ol(f);l.push(p)}return $v(0,null,z1(o,a),1,s,l,null,null,null,[r],null)}function z1(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function ND(t){let n=t[rv].kind;return n==="input"||n==="twoWay"}var uf=class extends tx{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Ba(e[Z],Oe),this.location=Xa(this._tNode,e),this.instance=Rn(this._tNode.index,e)[vt],this.hostView=this.changeDetectorRef=new Gr(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=i_(i,r[Z],r,n,e);this.previousInputValues.set(n,e);let a=Rn(i.index,r);Rf(a,1)}get injector(){return new zo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function $1(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Je=(()=>{class t{static __NG_ELEMENT_ID__=G1}return t})();function G1(){let t=Mt();return fx(t,re())}var ov=class t extends Je{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Xa(this._hostTNode,this._hostLView)}get injector(){return new zo(this._hostTNode,this._hostLView)}get parentInjector(){let n=Dv(this._hostTNode,this._hostLView);if(sw(n)){let e=nf(n,this._hostLView),i=tf(n),r=e[Z].data[i+8];return new zo(r,e)}else return new zo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=PD(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-st}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=Kl(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,Wo(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let l=n&&!Vk(n),c;if(l)c=e;else{let T=e||{};c=T.index,i=T.injector,r=T.projectableNodes,o=T.environmentInjector||T.ngModuleRef,a=T.directives,s=T.bindings}let u=l?n:new Wr(Zn(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let N=(l?f:this.parentInjector).get(ze,null);N&&(o=N)}let p=Zn(u.componentType??{}),h=Kl(this._lContainer,p?.id??null),_=h?.firstChild??null,x=u.create(f,r,_,o,a,s);return this.insertImpl(x.hostView,c,Wo(this._hostTNode,h)),x}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(FC(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[gt],c=new t(l,l[Qt],l[gt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return ns(a,r,o,i),n.attachToViewContainerRef(),$m(Ig(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=PD(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Zl(this._lContainer,e);i&&(Nl(Ig(this._lContainer),e),Mf(i[Z],i))}detach(n){let e=this._adjustIndex(n,-1),i=Zl(this._lContainer,e);return i&&Nl(Ig(this._lContainer),e)!=null?new Gr(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function PD(t){return t[Fl]}function Ig(t){return t[Fl]||(t[Fl]=[])}function fx(t,n){let e,i=n[t.index];return hn(i)?e=i:(e=GE(i,n,null,t),n[t.index]=e,Wv(n,e)),px(e,n,t,i),new ov(e,t,n)}function W1(t,n){let e=t[ke],i=e.createComment(""),r=An(n,t),o=e.parentNode(r);return sf(e,o,i,e.nextSibling(r),!1),i}var px=hx,u_=()=>!1;function q1(t,n,e){return u_(t,n,e)}function hx(t,n,e,i){if(t[Ii])return;let r;e.type&8?r=mn(i):r=W1(n,e),t[Ii]=r}function Y1(t,n,e){if(t[Ii]&&t[Jn])return!0;let i=e[Zt],r=n.index-Oe;if(!i||bw(n)||eE(i,r))return!1;let a=zg(i,r),s=i.data[bf]?.[r];if(s===void 0)return!1;let[l,c]=_1(a,s);return t[Ii]=l,t[Jn]=c,!0}function Q1(t,n,e,i){u_(t,e,n)||hx(t,n,e,i)}function mx(){px=Q1,u_=Y1}var av=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},sv=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)p_(n,e).matches!==null&&this.queries[e].setDirty()}},ff=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=eP(n):this.predicate=n}},lv=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},cv=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,Z1(e,o)),this.matchTNodeWithReadOption(n,e,Qu(e,n,o,!1,!1))}else i===Ne?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Qu(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===V||r===Je||r===Ne&&e.type&4)this.addMatch(e.index,-2);else{let o=Qu(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function Z1(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function K1(t,n){return t.type&11?Xa(t,n):t.type&4?kf(t,n):null}function X1(t,n,e,i){return e===-1?K1(n,t):e===-2?J1(t,n,i):Wl(t,t[Z],e,n)}function J1(t,n,e){if(e===V)return Xa(n,t);if(e===Ne)return kf(n,t);if(e===Je)return fx(n,t)}function gx(t,n,e,i){let r=n[Si].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let u=o[c];s.push(X1(n,u,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function dv(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=gx(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],u=n[-l];for(let f=st;f<u.length;f++){let p=u[f];p[Br]===p[gt]&&dv(p[Z],p,c,i)}if(u[jo]!==null){let f=u[jo];for(let p=0;p<f.length;p++){let h=f[p];dv(h[Z],h,c,i)}}}}}return i}function f_(t,n){return t[Si].queries[n].queryList}function vx(t,n,e){let i=new Ut((e&4)===4);return jC(t,n,i,i.destroy),(n[Si]??=new sv).queries.push(new av(i))-1}function _x(t,n,e){let i=Ye();return i.firstCreatePass&&(bx(i,new ff(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),vx(i,re(),n)}function yx(t,n,e,i){let r=Ye();if(r.firstCreatePass){let o=Mt();bx(r,new ff(n,e,i),o.index),tP(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return vx(r,re(),e)}function eP(t){return t.split(",").map(n=>n.trim())}function bx(t,n,e){t.queries===null&&(t.queries=new lv),t.queries.track(new cv(n,e))}function tP(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function p_(t,n){return t.queries.getByIndex(n)}function Cx(t,n){let e=t[Z],i=p_(e,n);return i.crossesNgTemplate?dv(e,t,n,[]):gx(e,t,i,n)}function Dx(t,n,e){let i,r=fl(()=>{i._dirtyCounter();let o=nP(i,t);if(n&&o===void 0)throw new k(-951,!1);return o});return i=r[ht],i._dirtyCounter=I(0),i._flatValue=void 0,r}function h_(t){return Dx(!0,!1,t)}function m_(t){return Dx(!0,!0,t)}function wx(t,n){let e=t[ht];e._lView=re(),e._queryIndex=n,e._queryList=f_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function nP(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[de]&4)return n?void 0:Ht;let r=f_(e,i),o=Cx(e,i);return r.reset(o,gw),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Ni=class{},jf=class{};var pf=class extends Ni{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new df(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Bm(n);this._bootstrapComponents=_E(o.bootstrap),this._r3Injector=gg(n,e,[{provide:Ni,useValue:this},{provide:lc,useValue:this.componentFactoryResolver},...i],Rl(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},hf=class extends jf{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new pf(this.moduleType,n,[])}};var Xl=class extends Ni{injector;componentFactoryResolver=new df(this);instance=null;constructor(n){super();let e=new No([...n.providers,{provide:Ni,useValue:this},{provide:lc,useValue:this.componentFactoryResolver}],n.parent||Fa(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function rs(t,n,e=null){return new Xl({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var iP=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Iu(!1,e.type),r=i.length>0?rs([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t(ne(ze))})}return t})();function O(t){return ec(()=>{let n=Ex(t),e=se(D({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===wv.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(iP).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||ri.Emulated,styles:t.styles||Ht,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&On("NgStandalone"),xx(e);let i=t.dependencies;return e.directiveDefs=FD(i,rP),e.pipeDefs=FD(i,Hm),e.id=sP(e),e})}function rP(t){return Zn(t)||Ol(t)}function F(t){return ec(()=>({type:t.type,bootstrap:t.bootstrap||Ht,declarations:t.declarations||Ht,imports:t.imports||Ht,exports:t.exports||Ht,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function oP(t,n){if(t==null)return Kn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=xf.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function aP(t){if(t==null)return Kn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function E(t){return ec(()=>{let n=Ex(t);return xx(n),n})}function g_(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Ex(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Kn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Ht,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:oP(t.inputs,n),outputs:aP(t.outputs),debugInfo:null}}function xx(t){t.features?.forEach(n=>n(t))}function FD(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function sP(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function v_(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=lP,e.hostDirectives=i?t.map(uv):[t]):i?e.hostDirectives.unshift(...t.map(uv)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function lP(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,Sx(a,n,i),r.set(a,[s,n.length-1])}o===0&&ei(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function Sx(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)LD(uv(o),n,e)}else LD(i,n,e)}function LD(t,n,e){let i=Ol(t.directive);cP(i.declaredInputs,t.inputs),Sx(i,n,e),e.set(i,t),n.push(i)}function uv(t){return typeof t=="function"?{directive:It(t),inputs:Kn,outputs:Kn}:{directive:It(t.directive),inputs:VD(t.inputs),outputs:VD(t.outputs)}}function VD(t){if(t===void 0||t.length===0)return Kn;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function cP(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function dP(t){return Object.getPrototypeOf(t.prototype).constructor}function te(t){let n=dP(t.type),e=!0,i=[t];for(;n;){let r;if(ei(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new k(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let a=t;a.inputs=Mg(t.inputs),a.declaredInputs=Mg(t.declaredInputs),a.outputs=Mg(t.outputs);let s=r.hostBindings;s&&mP(t,s);let l=r.viewQuery,c=r.contentQueries;if(l&&pP(t,l),c&&hP(t,c),uP(t,r),yC(t.outputs,r.outputs),ei(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let a=0;a<o.length;a++){let s=o[a];s&&s.ngInherit&&s(t),s===te&&(e=!1)}}n=Object.getPrototypeOf(n)}fP(i)}function uP(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function fP(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Ya(r.hostAttrs,e=Ya(e,r.hostAttrs))}}function Mg(t){return t===Kn?{}:t===Ht?[]:t}function pP(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function hP(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function mP(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function Ix(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=Ya(t.mergedAttrs,t.attrs);let u=t.tView=$v(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),Ha(t,!1);let l=Mx(e,n,t,i);ju()&&Kv(e,n,l,t),Qa(l,n);let c=GE(l,n,l,t);n[i+Oe]=c,Wv(n,c),q1(c,t,n)}function gP(t,n,e,i,r,o,a,s,l,c,u){let f=e+Oe,p;return n.firstCreatePass?(p=is(n,f,4,a||null,s||null),Ru()&&ix(n,t,p,gn(n.consts,c),Jv),rw(n,p)):p=n.data[f],Ix(p,t,n,e,i,r,o,l),ja(p)&&Af(n,t,p),c!=null&&sc(t,p,u),p}function Za(t,n,e,i,r,o,a,s,l,c,u){let f=e+Oe,p;if(n.firstCreatePass){if(p=is(n,f,4,a||null,s||null),c!=null){let h=gn(n.consts,c);p.localNames=[];for(let _=0;_<h.length;_+=2)p.localNames.push(h[_],-1)}}else p=n.data[f];return Ix(p,t,n,e,i,r,o,l),c!=null&&sc(t,p,u),p}function ue(t,n,e,i,r,o,a,s){let l=re(),c=Ye(),u=gn(c.consts,o);return gP(l,c,t,n,e,i,r,u,void 0,a,s),ue}function os(t,n,e,i,r,o,a,s){let l=re(),c=Ye(),u=gn(c.consts,o);return Za(l,c,t,n,e,i,r,u,void 0,a,s),os}var Mx=Tx;function Tx(t,n,e,i){return tr(!0),n[ke].createComment("")}function vP(t,n,e,i){let r=!wf(n,e);tr(r);let o=n[Zt]?.data[Tw]?.[i]??null;if(o!==null&&e.tView!==null&&e.tView.ssrId===null&&(e.tView.ssrId=o),r)return Tx(t,n);let a=n[Zt],s=Of(a,t,n,e);Df(a,i,s);let l=kv(a,i);return Nf(l,s)}function Ax(){Mx=vP}var _n=(function(t){return t[t.NOT_STARTED=0]="NOT_STARTED",t[t.IN_PROGRESS=1]="IN_PROGRESS",t[t.COMPLETE=2]="COMPLETE",t[t.FAILED=3]="FAILED",t})(_n||{}),jD=0,_P=1,Dt=(function(t){return t[t.Placeholder=0]="Placeholder",t[t.Loading=1]="Loading",t[t.Complete=2]="Complete",t[t.Error=3]="Error",t})(Dt||{});var yP=0,cc=1;var bP=4,CP=5;var DP=7,qa=8,wP=9,__=(function(t){return t[t.Manual=0]="Manual",t[t.Playthrough=1]="Playthrough",t})(__||{});function Ju(t,n){let e=xP(t),i=n[e];if(i!==null){for(let r of i)r();n[e]=null}}function EP(t){Ju(1,t),Ju(0,t),Ju(2,t)}function xP(t){let n=bP;return t===1?n=CP:t===2&&(n=wP),n}function Rx(t){return t+1}function as(t,n){let e=t[Z],i=Rx(n.index);return t[i]}function dc(t,n){let e=Rx(n.index);return t.data[e]}function SP(t,n,e){let i=n[Z],r=dc(i,e);switch(t){case Dt.Complete:return r.primaryTmplIndex;case Dt.Loading:return r.loadingTmplIndex;case Dt.Error:return r.errorTmplIndex;case Dt.Placeholder:return r.placeholderTmplIndex;default:return null}}function BD(t,n){return n===Dt.Placeholder?t.placeholderBlockConfig?.[jD]??null:n===Dt.Loading?t.loadingBlockConfig?.[jD]??null:null}function IP(t){return t.loadingBlockConfig?.[_P]??null}function HD(t,n){if(!t||t.length===0)return n;let e=new Set(t);for(let i of n)e.add(i);return t.length===e.size?t:Array.from(e)}function MP(t,n){let e=n.primaryTmplIndex+Oe;return Ba(t,e)}var TP=(()=>{class t{cachedInjectors=new Map;getOrCreateInjector(e,i,r,o){if(!this.cachedInjectors.has(e)){let a=r.length>0?rs(r,i,o):null;this.cachedInjectors.set(e,a)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:t,providedIn:"environment",factory:()=>new t})}return t})();var kx=new y("");function Tg(t,n,e){return t.get(TP).getOrCreateInjector(n,t,e,"")}function AP(t,n,e){if(t instanceof Wa){let r=t.injector,o=t.parentInjector,a=Tg(o,n,e);return new Wa(r,a)}let i=t.get(ze);if(i!==t){let r=Tg(i,n,e);return new Wa(t,r)}return Tg(t,n,e)}function Uo(t,n,e,i=!1){let r=e[gt],o=r[Z];if(Ur(r))return;let a=as(r,n),s=a[cc],l=a[DP];if(!(l!==null&&t<l)&&UD(s,t)&&UD(a[yP]??-1,t)){let c=dc(o,n),f=!i&&!0&&(IP(c)!==null||BD(c,Dt.Loading)!==null||BD(c,Dt.Placeholder))?OP:kP;try{f(t,a,e,n,r)}catch(p){n_(r,p)}}}function RP(t,n){let e=t[Jn]?.findIndex(r=>r.data[Ow]===n[cc])??-1;return{dehydratedView:e>-1?t[Jn][e]:null,dehydratedViewIx:e}}function kP(t,n,e,i,r){Fe(Te.DeferBlockStateStart);let o=SP(t,r,i);if(o!==null){n[cc]=t;let a=r[Z],s=o+Oe,l=Ba(a,s),c=0;o_(e,c);let u;if(t===Dt.Complete){let _=dc(a,i),x=_.providers;x&&x.length>0&&(u=AP(r[fn],_,x))}let{dehydratedView:f,dehydratedViewIx:p}=RP(e,n),h=ts(r,l,null,{injector:u,dehydratedView:f});if(ns(e,h,c,Wo(l,f)),Rf(h,2),p>-1&&e[Jn]?.splice(p,1),(t===Dt.Complete||t===Dt.Error)&&Array.isArray(n[qa])){for(let _ of n[qa])_();n[qa]=null}}Fe(Te.DeferBlockStateEnd)}function UD(t,n){return t<n}function zD(t,n,e){t.loadingPromise.then(()=>{t.loadingState===_n.COMPLETE?Uo(Dt.Complete,n,e):t.loadingState===_n.FAILED&&Uo(Dt.Error,n,e)})}var OP=null;var Bf=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function Pi(t){return typeof t=="function"&&t[ht]!==void 0}function y_(t){return Pi(t)&&typeof t.set=="function"}var b_=new y("");function qr(t){return!!t&&typeof t.then=="function"}function C_(t){return!!t&&typeof t.subscribe=="function"}var Ox=new y("");var D_=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(Ox,{optional:!0})??[];injector=d(J);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=kt(this.injector,r);if(qr(o))e.push(o);else if(C_(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Yr=new y("");function Nx(){tm(()=>{let t="";throw new k(600,t)})}function Px(t){return t.isBoundToModule}var NP=10;var Et=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(vn);afterRenderManager=d(If);zonelessEnabled=d(Hl);rootEffectScheduler=d(Hu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new S;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(Ri);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ce(e=>!e))}constructor(){d(ai,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(ze);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=J.NULL){return this._injector.get(K).run(()=>{Fe(Te.BootstrapComponentStart);let a=e instanceof Lf;if(!this._injector.get(D_).done){let _="";throw new k(405,_)}let l;a?l=e:l=this._injector.get(lc).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=Px(l)?void 0:this._injector.get(Ni),u=i||l.selector,f=l.create(r,[],u,c),p=f.location.nativeElement,h=f.injector.get(b_,null);return h?.registerApplication(p),f.onDestroy(()=>{this.detachView(f.hostView),Gl(this.components,f),h?.unregisterApplication(p)}),this._loadComponent(f),Fe(Te.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Fe(Te.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Sf.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Fe(Te.ChangeDetectionEnd),new k(101,!1);let e=ie(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ie(e),this.afterTick.next(),Fe(Te.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(wt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<NP;){Fe(Te.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Fe(Te.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ll(r))continue;let o=i&&!this.zonelessEnabled?0:1;HE(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Ll(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Gl(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Yr,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Gl(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new k(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Gl(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function w_(){let t,n;return{promise:new Promise((i,r)=>{t=i,n=r}),resolve:t,reject:n}}function Fx(t,n,e){let i=n[fn],r=n[Z];if(t.loadingState!==_n.NOT_STARTED)return t.loadingPromise??Promise.resolve();let o=as(n,e),a=MP(r,t);t.loadingState=_n.IN_PROGRESS,Ju(1,o);let s=t.dependencyResolverFn,l=i.get(za).add();return s?(t.loadingPromise=Promise.allSettled(s()).then(c=>{let u=!1,f=null,p=[],h=[];for(let _=0;_<c.length;_++){let x=c[_];if(x.status==="fulfilled"){let T=x.value,N=Zn(T)||Ol(T);if(N)p.push(N);else{let pe=Hm(T);pe&&h.push(pe)}}else{u=!0,f=x.reason instanceof Error?x.reason:new Error(String(x.reason));break}}if(u){if(t.loadingState=_n.FAILED,t.errorTmplIndex===null){let x="",T=new k(-750,x);n_(n,T)}}else{t.loadingState=_n.COMPLETE;let _=a.tView;if(p.length>0){_.directiveRegistry=HD(_.directiveRegistry,p);let x=p.map(N=>N.type),T=Iu(!1,...x);t.providers=T}h.length>0&&(_.pipeRegistry=HD(_.pipeRegistry,h))}}),t.loadingPromise.finally(()=>{t.loadingPromise=null,l()})):(t.loadingPromise=Promise.resolve().then(()=>{t.loadingPromise=null,t.loadingState=_n.COMPLETE,l()}),t.loadingPromise)}function PP(t,n){return n[fn].get(kx,null,{optional:!0})?.behavior!==__.Manual}function FP(t,n,e){let i=n[Z],r=n[e.index];if(!PP(t,n))return;let o=as(n,e),a=dc(i,e);switch(EP(o),a.loadingState){case _n.NOT_STARTED:Uo(Dt.Loading,e,r),Fx(a,n,e),a.loadingState===_n.IN_PROGRESS&&zD(a,e,r);break;case _n.IN_PROGRESS:Uo(Dt.Loading,e,r),zD(a,e,r);break;case _n.COMPLETE:Uo(Dt.Complete,e,r);break;case _n.FAILED:Uo(Dt.Error,e,r);break;default:}}async function Lx(t,n,e){let i=t.get(Cf);if(i.hydrating.has(n))return;let{parentBlockPromise:o,hydrationQueue:a}=DO(n,t);if(a.length===0)return;o!==null&&a.shift(),jP(i,a),o!==null&&await o;let s=a[0];i.has(s)?await $D(t,a,e):i.awaitParentBlock(s,async()=>await $D(t,a,e))}async function $D(t,n,e){let i=t.get(Cf),r=i.hydrating,o=t.get(Ri),a=o.add();for(let l=0;l<n.length;l++){let c=n[l],u=i.get(c);if(u!=null){if(await HP(u),await BP(t),LP(u)){g1(u),GD(n.slice(l),i);break}r.get(c).resolve()}else{VP(l,n,i),GD(n.slice(l),i);break}}let s=n[n.length-1];await r.get(s)?.promise,o.remove(a),e&&e(n),v1(i.get(s),n,i,t.get(Et))}function LP(t){return as(t.lView,t.tNode)[cc]===Dt.Error}function VP(t,n,e){let i=t-1,r=i>-1?e.get(n[i]):null;r&&Ff(r.lContainer)}function GD(t,n){let e=n.hydrating;for(let i in t)e.get(i)?.reject();n.cleanup(t)}function jP(t,n){for(let e of n)t.hydrating.set(e,w_())}function BP(t){return new Promise(n=>lt(n,{injector:t}))}async function HP(t){let{tNode:n,lView:e}=t,i=as(e,n);return new Promise(r=>{UP(i,r),FP(2,e,n)})}function UP(t,n){Array.isArray(t[qa])||(t[qa]=[]),t[qa].push(n)}function oe(t,n,e,i){let r=re(),o=zr();if(yn(r,o,n)){let a=Ye(),s=Bl();VN(s,r,t,n,e,i)}return oe}var fv=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Ag(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function zP(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){ie(i);let c=n.length-1;for(ie(null);a<=s&&a<=c;){let u=t.at(a),f=n[a],p=Ag(a,u,a,f,e);if(p!==0){p<0&&t.updateValue(a,f),a++;continue}let h=t.at(s),_=n[c],x=Ag(s,h,c,_,e);if(x!==0){x<0&&t.updateValue(s,_),s--,c--;continue}let T=e(a,u),N=e(s,h),pe=e(a,f);if(Object.is(pe,N)){let pt=e(c,_);Object.is(pt,T)?(t.swap(a,s),t.updateValue(s,_),c--,s--):t.move(s,a),t.updateValue(a,f),a++;continue}if(r??=new mf,o??=qD(t,a,s,e),pv(t,r,a,pe))t.updateValue(a,f),a++,s++;else if(o.has(pe))r.set(T,t.detach(a)),s--;else{let pt=t.create(a,n[a]);t.attach(a,pt),a++,s++}}for(;a<=c;)WD(t,r,e,a,n[a]),a++}else if(n!=null){ie(i);let c=n[Symbol.iterator]();ie(null);let u=c.next();for(;!u.done&&a<=s;){let f=t.at(a),p=u.value,h=Ag(a,f,a,p,e);if(h!==0)h<0&&t.updateValue(a,p),a++,u=c.next();else{r??=new mf,o??=qD(t,a,s,e);let _=e(a,p);if(pv(t,r,a,_))t.updateValue(a,p),a++,s++,u=c.next();else if(!o.has(_))t.attach(a,t.create(a,p)),a++,s++,u=c.next();else{let x=e(a,f);r.set(x,t.detach(a)),s--}}}for(;!u.done;)WD(t,r,e,t.length,u.value),u=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function pv(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function WD(t,n,e,i,r){if(pv(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function qD(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var mf=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function A(t,n,e,i,r,o,a,s){On("NgControlFlow");let l=re(),c=Ye(),u=gn(c.consts,o);return Za(l,c,t,n,e,i,r,u,256,a,s),E_}function E_(t,n,e,i,r,o,a,s){On("NgControlFlow");let l=re(),c=Ye(),u=gn(c.consts,o);return Za(l,c,t,n,e,i,r,u,512,a,s),E_}function R(t,n){On("NgControlFlow");let e=re(),i=zr(),r=e[i]!==Xt?e[i]:-1,o=r!==-1?gf(e,Oe+r):void 0,a=0;if(yn(e,i,t)){let s=ie(null);try{if(o!==void 0&&o_(o,a),t!==-1){let l=Oe+t,c=gf(e,l),u=vv(e[Z],l),f=JE(c,u,e),p=ts(e,u,n,{dehydratedView:f});ns(c,p,a,Wo(u,f))}}finally{ie(s)}}else if(o!==void 0){let s=WE(o,a);s!==void 0&&(s[vt]=n)}}var hv=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-st}};function tt(t,n){return n}var mv=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function nt(t,n,e,i,r,o,a,s,l,c,u,f,p){On("NgControlFlow");let h=re(),_=Ye(),x=l!==void 0,T=re(),N=s?a.bind(T[Nt][vt]):a,pe=new mv(x,N);T[Oe+t]=pe,Za(h,_,t+1,n,e,i,r,gn(_.consts,o),256),x&&Za(h,_,t+2,l,c,u,f,gn(_.consts,p),512)}var gv=class extends fv{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-st}at(n){return this.getLView(n)[vt].$implicit}attach(n,e){let i=e[Zt];this.needsIndexUpdate||=n!==this.length,ns(this.lContainer,e,n,Wo(this.templateTNode,i)),$P(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,GP(this.lContainer,n),WP(this.lContainer,n)}create(n,e){let i=Kl(this.lContainer,this.templateTNode.tView.ssrId);return ts(this.hostLView,this.templateTNode,new hv(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Mf(n[Z],n)}updateValue(n,e){this.getLView(n)[vt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[vt].$index=n}getLView(n){return qP(this.lContainer,n)}};function it(t){let n=ie(null),e=Ti();try{let i=re(),r=i[Z],o=i[e],a=e+1,s=gf(i,a);if(o.liveCollection===void 0){let c=vv(r,a);o.liveCollection=new gv(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(zP(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=zr(),u=l.length===0;if(yn(i,c,u)){let f=e+2,p=gf(i,f);if(u){let h=vv(r,f),_=JE(p,h,i),x=ts(i,h,void 0,{dehydratedView:_});ns(p,x,0,Wo(h,_))}else r.firstUpdatePass&&Pf(p),o_(p,0)}}}finally{ie(n)}}function gf(t,n){return t[n]}function $P(t,n){if(t.length<=st)return;let e=st+n,i=t[e],r=i?i[Hr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[fn];gN(o,r),Go.delete(i[er]),r.detachedLeaveAnimationFns=void 0}}function GP(t,n){if(t.length<=st)return;let e=st+n,i=t[e],r=i?i[Hr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function WP(t,n){return Zl(t,n)}function qP(t,n){return WE(t,n)}function vv(t,n){return Ba(t,n)}function M(t,n,e){let i=re(),r=zr();if(yn(i,r,n)){let o=Ye(),a=Bl();NE(a,i,t,n,i[ke],e)}return M}function _v(t,n,e,i,r){i_(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=re(),o=r[Z],a=t+Oe,s=o.firstCreatePass?s_(a,r,2,n,Jv,Ru(),e,i):o.data[a];if(Mi(s)){let l=r[Xn].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(dx(c),()=>(YD(t,n,r,s,i),m))}}return YD(t,n,r,s,i),m}function YD(t,n,e,i,r){if(e_(i,e,t,n,x_),ja(i)){let o=e[Z];Af(o,e,i),Ov(o,i,e)}r!=null&&sc(e,i)}function v(){let t=Ye(),n=Mt(),e=t_(n);return t.firstCreatePass&&l_(t,e),lg(e)&&cg(),sg(),e.classesWithoutHost!=null&&$k(e)&&_v(t,e,re(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Gk(e)&&_v(t,e,re(),e.stylesWithoutHost,!1),v}function X(t,n,e,i){return m(t,n,e,i),v(),X}function _t(t,n,e,i){let r=re(),o=r[Z],a=t+Oe,s=o.firstCreatePass?k1(a,o,2,n,e,i):o.data[a];return e_(s,r,t,n,x_),i!=null&&sc(r,s),_t}function At(){let t=Mt(),n=t_(t);return lg(n)&&cg(),sg(),At}function zt(t,n,e,i){return _t(t,n,e,i),At(),zt}var x_=(t,n,e,i,r)=>(tr(!0),Bv(n[ke],i,Vu()));function YP(t,n,e,i,r){let o=!wf(n,e);if(tr(o),o)return Bv(n[ke],i,Vu());let a=n[Zt],s=Of(a,t,n,e);return Jw(a,r)&&Df(a,r,s.nextSibling),a&&(_w(e)||yw(s))&&Mi(e)&&(UC(e),fE(s)),s}function Vx(){x_=YP}function dt(t,n,e){let i=re(),r=i[Z],o=t+Oe,a=r.firstCreatePass?s_(o,i,8,"ng-container",Jv,Ru(),n,e):r.data[o];if(e_(a,i,t,"ng-container",jx),ja(a)){let s=i[Z];Af(s,i,a),Ov(s,a,i)}return e!=null&&sc(i,a),dt}function ut(){let t=Ye(),n=Mt(),e=t_(n);return t.firstCreatePass&&l_(t,e),ut}function Qe(t,n,e){return dt(t,n,e),ut(),Qe}var jx=(t,n,e,i,r)=>(tr(!0),dE(n[ke],""));function QP(t,n,e,i,r){let o,a=!wf(n,e);if(tr(a),a)return dE(n[ke],"");let s=n[Zt],l=Of(s,t,n,e),c=bO(s,r);return Df(s,r,l),o=Nf(c,l),o}function Bx(){jx=QP}function rt(){return re()}function yt(t,n,e){let i=re(),r=zr();if(yn(i,r,n)){let o=Ye(),a=Bl();PE(a,i,t,n,i[ke],e)}return yt}var Ul=void 0;function ZP(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var KP=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Ul,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Ul,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Ul,Ul,Ul],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",ZP],Rg={};function uc(t){let n=XP(t),e=QD(n);if(e)return e;let i=n.split("-")[0];if(e=QD(i),e)return e;if(i==="en")return KP;throw new k(701,!1)}function QD(t){return t in Rg||(Rg[t]=In.ng&&In.ng.common&&In.ng.common.locales&&In.ng.common.locales[t]),Rg[t]}var Qr=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(Qr||{});function XP(t){return t.toLowerCase().replace(/_/g,"-")}var fc="en-US",JP="USD";var eF=fc;function Hx(t){typeof t=="string"&&(eF=t.toLowerCase().replace(/_/g,"-"))}function G(t,n,e){let i=re(),r=Ye(),o=Mt();return Ux(r,i,i[ke],o,t,n,e),G}function Hf(t,n,e){let i=re(),r=Ye(),o=Mt();return(o.type&3||e)&&lx(o,r,i,e,i[ke],t,n,Xu(o,i,n)),Hf}function Ux(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=Xu(i,n,o),lx(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let p=u[f],h=u[f+1];l??=Xu(i,n,o),OD(i,n,p,h,r,l)}if(c&&c.length)for(let f of c)l??=Xu(i,n,o),OD(i,n,f,r,r,l)}}function C(t=1){return eD(t)}function tF(t,n){let e=null,i=tN(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?bE(t,o,!0):rN(i,o))return r}return e}function De(t){let n=re()[Nt][Qt];if(!n.projection){let e=t?t.length:1,i=n.projection=xC(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?tF(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function W(t,n=0,e,i,r,o){let a=re(),s=Ye(),l=i?t+1:null;l!==null&&Za(a,s,l,i,r,o,null,e);let c=is(s,Oe+t,16,null,e||null);c.projection===null&&(c.projection=n),fg();let f=!a[Zt]||ku();a[Nt][Qt].projection[c.projection]===null&&l!==null?nF(a,s,l):f&&!rc(c)&&IN(s,a,c)}function nF(t,n,e){let i=Oe+e,r=n.data[i],o=t[i],a=Kl(o,r.tView.ssrId),s=ts(t,r,void 0,{dehydratedView:a});ns(o,s,0,Wo(r,a))}function Ze(t,n,e,i){return yx(t,n,e,i),Ze}function et(t,n,e){return _x(t,n,e),et}function U(t){let n=re(),e=Ye(),i=Pu();jl(i+1);let r=p_(e,i);if(t.dirty&&PC(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Cx(n,i);t.reset(o,gw),t.notifyOnChanges()}return!0}return!1}function z(){return f_(re(),Pu())}function Uf(t,n,e,i,r){return wx(n,yx(t,e,i,r)),Uf}function zf(t,n,e,i){return wx(t,_x(n,e,i)),zf}function $f(t=1){jl(Pu()+t)}function Pt(t){let n=$C();return eg(n,Oe+t)}function Wu(t,n){return t<<17|n<<2}function qo(t){return t>>17&32767}function iF(t){return(t&2)==2}function rF(t,n){return t&131071|n<<17}function yv(t){return t|2}function Ka(t){return(t&131068)>>2}function kg(t,n){return t&-131069|n<<2}function oF(t){return(t&1)===1}function bv(t){return t|1}function aF(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=qo(a),l=Ka(a);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||Pa(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let p=qo(t[s+1]);t[i+1]=Wu(p,s),p!==0&&(t[p+1]=kg(t[p+1],i)),t[s+1]=rF(t[s+1],i)}else t[i+1]=Wu(s,0),s!==0&&(t[s+1]=kg(t[s+1],i)),s=i;else t[i+1]=Wu(l,0),s===0?s=i:t[l+1]=kg(t[l+1],i),l=i;c&&(t[i+1]=yv(t[i+1])),ZD(t,u,i,!0),ZD(t,u,i,!1),sF(n,u,t,i,o),a=Wu(s,l),o?n.classBindings=a:n.styleBindings=a}function sF(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Pa(o,n)>=0&&(e[i+1]=bv(e[i+1]))}function ZD(t,n,e,i){let r=t[e+1],o=n===null,a=i?qo(r):Ka(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];lF(l,n)&&(s=!0,t[a+1]=i?bv(c):yv(c)),a=i?qo(c):Ka(c)}s&&(t[e+1]=i?yv(r):bv(r))}function lF(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Pa(t,n)>=0:!1}var ii={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function cF(t){return t.substring(ii.key,ii.keyEnd)}function dF(t){return uF(t),zx(t,$x(t,0,ii.textEnd))}function zx(t,n){let e=ii.textEnd;return e===n?-1:(n=ii.keyEnd=fF(t,ii.key=n,e),$x(t,n,e))}function uF(t){ii.key=0,ii.keyEnd=0,ii.value=0,ii.valueEnd=0,ii.textEnd=t.length}function $x(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function fF(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function we(t,n,e){return Gx(t,n,e,!1),we}function Y(t,n){return Gx(t,n,null,!0),Y}function Ft(t){hF(bF,pF,t,!0)}function pF(t,n){for(let e=dF(n);e>=0;e=zx(n,e))xu(t,cF(n),!0)}function Gx(t,n,e,i){let r=re(),o=Ye(),a=Ou(2);if(o.firstUpdatePass&&qx(o,t,a,i),n!==Xt&&yn(r,a,n)){let s=o.data[Ti()];Yx(o,s,r,r[ke],t,r[a+1]=DF(n,e),i,a)}}function hF(t,n,e,i){let r=Ye(),o=Ou(2);r.firstUpdatePass&&qx(r,null,o,i);let a=re();if(e!==Xt&&yn(a,o,e)){let s=r.data[Ti()];if(Qx(s,i)&&!Wx(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=bu(l,e||"")),_v(r,s,a,e,i)}else CF(r,s,a,a[ke],a[o+1],a[o+1]=yF(t,n,e),i,o)}}function Wx(t,n){return n>=t.expandoStartIndex}function qx(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Ti()],a=Wx(t,e);Qx(o,i)&&n===null&&!a&&(n=!1),n=mF(r,o,n,i),aF(r,o,n,e,a,i)}}function mF(t,n,e,i){let r=ZC(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Og(null,t,n,e,i),e=Jl(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=Og(r,t,n,e,i),o===null){let l=gF(t,n,i);l!==void 0&&Array.isArray(l)&&(l=Og(null,t,n,l[1],i),l=Jl(l,n.attrs,i),vF(t,n,i,l))}else o=_F(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function gF(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Ka(i)!==0)return t[qo(i)]}function vF(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[qo(r)]=i}function _F(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Jl(i,a,e)}return Jl(i,n.attrs,e)}function Og(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Jl(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Jl(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),xu(t,a,e?!0:n[++o]))}return t===void 0?null:t}function yF(t,n,e){if(e==null||e==="")return Ht;let i=[],r=oi(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function bF(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&xu(t,i,e)}function CF(t,n,e,i,r,o,a,s){r===Xt&&(r=Ht);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let p=l<r.length?r[l+1]:void 0,h=c<o.length?o[c+1]:void 0,_=null,x;u===f?(l+=2,c+=2,p!==h&&(_=f,x=h)):f===null||u!==null&&u<f?(l+=2,_=u):(c+=2,_=f,x=h),_!==null&&Yx(t,n,e,i,_,x,a,s),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function Yx(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],u=oF(c)?KD(l,n,e,r,Ka(c),a):void 0;if(!vf(u)){vf(o)||iF(c)&&(o=KD(l,null,e,r,s,a));let f=Jm(Ti(),e);TN(i,a,f,r,o)}}function KD(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,p=e[r+1];p===Xt&&(p=f?Ht:void 0);let h=f?Su(p,i):u===i?p:void 0;if(c&&!vf(h)&&(h=Su(l,i)),vf(h)&&(s=h,a))return s;let _=t[r+1];r=a?qo(_):Ka(_)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=Su(l,i))}return s}function vf(t){return t!==void 0}function DF(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Rl(oi(t)))),t}function Qx(t,n){return(t.flags&(n?8:16))!==0}function b(t,n=""){let e=re(),i=Ye(),r=t+Oe,o=i.firstCreatePass?is(i,r,1,n,null):i.data[r],a=Zx(i,e,o,n);e[r]=a,ju()&&Kv(i,e,a,o),Ha(o,!1)}var Zx=(t,n,e,i)=>(tr(!0),cE(n[ke],i));function wF(t,n,e,i){let r=!wf(n,e);if(tr(r),r)return cE(n[ke],i);let o=n[Zt];return Of(o,t,n,e)}function Kx(){Zx=wF}function Xx(t,n,e,i=""){return yn(t,zr(),e)?n+Na(e)+i:Xt}function EF(t,n,e,i,r,o=""){let a=GC(),s=sx(t,a,e,r);return Ou(2),s?n+Na(e)+i+Na(r)+o:Xt}function fe(t){return Se("",t),fe}function Se(t,n,e){let i=re(),r=Xx(i,t,n,e);return r!==Xt&&Jx(i,Ti(),r),Se}function ss(t,n,e,i,r){let o=re(),a=EF(o,t,n,e,i,r);return a!==Xt&&Jx(o,Ti(),a),ss}function Jx(t,n,e){let i=Jm(n,t);GO(t[ke],i,e)}function Gf(t,n,e){y_(n)&&(n=n());let i=re(),r=zr();if(yn(i,r,n)){let o=Ye(),a=Bl();NE(a,i,t,n,i[ke],e)}return Gf}function S_(t,n){let e=y_(t);return e&&t.set(n),e}function Wf(t,n){let e=re(),i=Ye(),r=Mt();return Ux(i,e,e[ke],r,t,n),Wf}function I_(t,n,e=""){return Xx(re(),t,n,e)}function XD(t,n,e){let i=Ye();i.firstCreatePass&&eS(n,i.data,i.blueprint,ei(t),e)}function eS(t,n,e,i,r){if(t=It(t),Array.isArray(t))for(let o=0;o<t.length;o++)eS(t[o],n,e,i,r);else{let o=Ye(),a=re(),s=Mt(),l=Oo(t)?t:It(t.provide),c=Ym(t),u=s.providerIndexes&1048575,f=s.directiveStart,p=s.providerIndexes>>20;if(Oo(t)||!t.multi){let h=new $o(c,r,ee,null),_=Pg(l,n,r?u:u+p,f);_===-1?(Lg(of(s,a),o,l),Ng(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(h),a.push(h)):(e[_]=h,a[_]=h)}else{let h=Pg(l,n,u+p,f),_=Pg(l,n,u,u+p),x=h>=0&&e[h],T=_>=0&&e[_];if(r&&!T||!r&&!x){Lg(of(s,a),o,l);let N=IF(r?SF:xF,e.length,r,i,c,t);!r&&T&&(e[_].providerFactory=N),Ng(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(N),a.push(N)}else{let N=tS(e[r?_:h],c,!r&&i);Ng(o,t,h>-1?h:_,N)}!r&&i&&T&&e[_].componentProviders++}}}function Ng(t,n,e,i){let r=Oo(n),o=RC(n);if(r||o){let l=(o?It(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function tS(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Pg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function xF(t,n,e,i,r){return Cv(this.multi,[])}function SF(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=Wl(i,i[Z],this.providerFactory.index,r);a=l.slice(0,s),Cv(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],Cv(o,a);return a}function Cv(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function IF(t,n,e,i,r,o){let a=new $o(t,e,ee,null);return a.multi=[],a.index=n,a.componentProviders=0,tS(a,r,i&&!e),a}function _e(t,n){return e=>{e.providersResolver=(i,r)=>XD(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>XD(i,r?r(n):n,!0))}}function Fi(t,n){let e=Vl()+t,i=re();return i[e]===Xt?d_(i,e,n()):O1(i,e)}function ls(t,n,e){return iS(re(),Vl(),t,n,e)}function M_(t,n,e,i,r){return MF(re(),Vl(),t,n,e,i,r)}function nS(t,n){let e=t[n];return e===Xt?void 0:e}function iS(t,n,e,i,r,o){let a=n+e;return yn(t,a,r)?d_(t,a+1,o?i.call(o,r):i(r)):nS(t,a+1)}function MF(t,n,e,i,r,o,a,s){let l=n+e;return N1(t,l,r,o,a)?d_(t,l+3,s?i.call(s,r,o,a):i(r,o,a)):nS(t,l+3)}function Li(t,n){let e=Ye(),i,r=t+Oe;e.firstCreatePass?(i=TF(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Lr(i.type,!0)),a,s=Wt(ee);try{let l=rf(!1),c=o();return rf(l),tg(e,re(),r,c),c}finally{Wt(s)}}function TF(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Vi(t,n,e){let i=t+Oe,r=re(),o=eg(r,i);return AF(r,i)?iS(r,Vl(),n,o.transform,e,o):o.transform(e)}function AF(t,n){return t[Z].data[n].pure}function cs(t,n){return kf(t,n)}var _f=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},T_=(()=>{class t{compileModuleSync(e){return new hf(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=Bm(e),o=_E(r.declarations).reduce((a,s)=>{let l=Zn(s);return l&&a.push(new Wr(l)),a},[]);return new _f(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var rS=(()=>{class t{applicationErrorHandler=d(vn);appRef=d(Et);taskService=d(Ri);ngZone=d(K);zonelessEnabled=d(Hl);tracing=d(ai,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new le;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Tl):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Cg,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?rD:vg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Tl+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function oS(){return[{provide:Qn,useExisting:rS},{provide:K,useClass:Al},{provide:Hl,useValue:!0}]}function RF(){return typeof $localize<"u"&&$localize.locale||fc}var pc=new y("",{factory:()=>d(pc,{optional:!0,skipSelf:!0})||RF()}),A_=new y("",{factory:()=>JP});function Le(t){return hC(t)}function xt(t,n){return fl(t,n?.equal)}var kF=t=>t;function qf(t,n){if(typeof t=="function"){let e=bm(t,kF,n?.equal);return aS(e,n?.debugName)}else{let e=bm(t.source,t.computation,t.equal);return aS(e,t.debugName)}}function aS(t,n){let e=t[ht],i=t;return i.set=r=>fC(e,r),i.update=r=>pC(e,r),i.asReadonly=Bu.bind(t),i}var P_={JSACTION:"__jsaction",OWNER:"__owner"},dS={};function OF(t){return t[P_.JSACTION]}function sS(t,n){t[P_.JSACTION]=n}function NF(t){return dS[t]}function PF(t,n){dS[t]=n}var ae={CLICK:"click",CLICKMOD:"clickmod",DBLCLICK:"dblclick",FOCUS:"focus",FOCUSIN:"focusin",BLUR:"blur",FOCUSOUT:"focusout",SUBMIT:"submit",KEYDOWN:"keydown",KEYPRESS:"keypress",KEYUP:"keyup",MOUSEOVER:"mouseover",MOUSEOUT:"mouseout",MOUSEENTER:"mouseenter",MOUSELEAVE:"mouseleave",POINTEROVER:"pointerover",POINTEROUT:"pointerout",POINTERENTER:"pointerenter",POINTERLEAVE:"pointerleave",ERROR:"error",LOAD:"load",TOUCHSTART:"touchstart",TOUCHEND:"touchend",TOUCHMOVE:"touchmove",TOGGLE:"toggle"},FF=[ae.MOUSEENTER,ae.MOUSELEAVE,"pointerenter","pointerleave"],vY=[ae.CLICK,ae.DBLCLICK,ae.FOCUSIN,ae.FOCUSOUT,ae.KEYDOWN,ae.KEYUP,ae.KEYPRESS,ae.MOUSEOVER,ae.MOUSEOUT,ae.SUBMIT,ae.TOUCHSTART,ae.TOUCHEND,ae.TOUCHMOVE,"touchcancel","auxclick","change","compositionstart","compositionupdate","compositionend","beforeinput","input","select","copy","cut","paste","mousedown","mouseup","wheel","contextmenu","dragover","dragenter","dragleave","drop","dragstart","dragend","pointerdown","pointermove","pointerup","pointercancel","pointerover","pointerout","gotpointercapture","lostpointercapture","ended","loadedmetadata","pagehide","pageshow","visibilitychange","beforematch"],LF=[ae.FOCUS,ae.BLUR,ae.ERROR,ae.LOAD,ae.TOGGLE],F_=t=>LF.indexOf(t)>=0;function VF(t){return t===ae.MOUSEENTER?ae.MOUSEOVER:t===ae.MOUSELEAVE?ae.MOUSEOUT:t===ae.POINTERENTER?ae.POINTEROVER:t===ae.POINTERLEAVE?ae.POINTEROUT:t}function jF(t,n,e,i){let r=!1;F_(n)&&(r=!0);let o=typeof i=="boolean"?{capture:r,passive:i}:r;return t.addEventListener(n,e,o),{eventType:n,handler:e,capture:r,passive:i}}function BF(t,n){if(t.removeEventListener){let e=typeof n.passive=="boolean"?{capture:n.capture}:n.capture;t.removeEventListener(n.eventType,n.handler,e)}else t.detachEvent&&t.detachEvent(`on${n.eventType}`,n.handler)}function HF(t){t.preventDefault?t.preventDefault():t.returnValue=!1}var lS=typeof navigator<"u"&&/Macintosh/.test(navigator.userAgent);function UF(t){return t.which===2||t.which==null&&t.button===4}function zF(t){return lS&&t.metaKey||!lS&&t.ctrlKey||UF(t)||t.shiftKey}function $F(t,n,e){let i=t.relatedTarget;return(t.type===ae.MOUSEOVER&&n===ae.MOUSEENTER||t.type===ae.MOUSEOUT&&n===ae.MOUSELEAVE||t.type===ae.POINTEROVER&&n===ae.POINTERENTER||t.type===ae.POINTEROUT&&n===ae.POINTERLEAVE)&&(!i||i!==e&&!e.contains(i))}function GF(t,n){let e={};for(let i in t){if(i==="srcElement"||i==="target")continue;let r=i,o=t[r];typeof o!="function"&&(e[r]=o)}return t.type===ae.MOUSEOVER?e.type=ae.MOUSEENTER:t.type===ae.MOUSEOUT?e.type=ae.MOUSELEAVE:t.type===ae.POINTEROVER?e.type=ae.POINTERENTER:e.type=ae.POINTERLEAVE,e.target=e.srcElement=n,e.bubbles=!1,e._originalEvent=t,e}var Kf=class{element;handlerInfos=[];constructor(n){this.element=n}addEventListener(n,e,i){this.handlerInfos.push(jF(this.element,n,e(this.element),i))}cleanUp(){for(let n=0;n<this.handlerInfos.length;n++)BF(this.element,this.handlerInfos[n]);this.handlerInfos=[]}},WF={EVENT_ACTION_SEPARATOR:":"};function Zr(t){return t.eventType}function L_(t,n){t.eventType=n}function Qf(t){return t.event}function uS(t,n){t.event=n}function fS(t){return t.targetElement}function pS(t,n){t.targetElement=n}function hS(t){return t.eic}function qF(t,n){t.eic=n}function YF(t){return t.timeStamp}function QF(t,n){t.timeStamp=n}function Zf(t){return t.eia}function mS(t,n,e){t.eia=[n,e]}function R_(t){t.eia=void 0}function Yf(t){return t[1]}function ZF(t){return t.eirp}function gS(t,n){t.eirp=n}function vS(t){return t.eir}function _S(t,n){t.eir=n}function yS(t){return{eventType:t.eventType,event:t.event,targetElement:t.targetElement,eic:t.eic,eia:t.eia,timeStamp:t.timeStamp,eirp:t.eirp,eiack:t.eiack,eir:t.eir}}function KF(t,n,e,i,r,o,a,s){return{eventType:t,event:n,targetElement:e,eic:i,timeStamp:r,eia:o,eirp:a,eiack:s}}var k_=class t{eventInfo;constructor(n){this.eventInfo=n}getEventType(){return Zr(this.eventInfo)}setEventType(n){L_(this.eventInfo,n)}getEvent(){return Qf(this.eventInfo)}setEvent(n){uS(this.eventInfo,n)}getTargetElement(){return fS(this.eventInfo)}setTargetElement(n){pS(this.eventInfo,n)}getContainer(){return hS(this.eventInfo)}setContainer(n){qF(this.eventInfo,n)}getTimestamp(){return YF(this.eventInfo)}setTimestamp(n){QF(this.eventInfo,n)}getAction(){let n=Zf(this.eventInfo);if(n)return{name:n[0],element:n[1]}}setAction(n){if(!n){R_(this.eventInfo);return}mS(this.eventInfo,n.name,n.element)}getIsReplay(){return ZF(this.eventInfo)}setIsReplay(n){gS(this.eventInfo,n)}getResolved(){return vS(this.eventInfo)}setResolved(n){_S(this.eventInfo,n)}clone(){return new t(yS(this.eventInfo))}},XF={},JF=/\s*;\s*/,eL=ae.CLICK,O_=class{a11yClickSupport=!1;clickModSupport=!0;syntheticMouseEventSupport;updateEventInfoForA11yClick=void 0;preventDefaultForA11yClick=void 0;populateClickOnlyAction=void 0;constructor({syntheticMouseEventSupport:n=!1,clickModSupport:e=!0}={}){this.syntheticMouseEventSupport=n,this.clickModSupport=e}resolveEventType(n){this.clickModSupport&&Zr(n)===ae.CLICK&&zF(Qf(n))?L_(n,ae.CLICKMOD):this.a11yClickSupport&&this.updateEventInfoForA11yClick(n)}resolveAction(n){vS(n)||(this.populateAction(n,fS(n)),_S(n,!0))}resolveParentAction(n){let e=Zf(n),i=e&&Yf(e);R_(n);let r=i&&this.getParentNode(i);r&&this.populateAction(n,r)}populateAction(n,e){let i=e;for(;i&&i!==hS(n)&&(i.nodeType===Node.ELEMENT_NODE&&this.populateActionOnElement(i,n),!Zf(n));)i=this.getParentNode(i);let r=Zf(n);if(r&&(this.a11yClickSupport&&this.preventDefaultForA11yClick(n),this.syntheticMouseEventSupport&&(Zr(n)===ae.MOUSEENTER||Zr(n)===ae.MOUSELEAVE||Zr(n)===ae.POINTERENTER||Zr(n)===ae.POINTERLEAVE)))if($F(Qf(n),Zr(n),Yf(r))){let o=GF(Qf(n),Yf(r));uS(n,o),pS(n,Yf(r))}else R_(n)}getParentNode(n){let e=n[P_.OWNER];if(e)return e;let i=n.parentNode;return i?.nodeName==="#document-fragment"?i?.host??null:i}populateActionOnElement(n,e){let i=this.parseActions(n),r=i[Zr(e)];r!==void 0&&mS(e,r,n),this.a11yClickSupport&&this.populateClickOnlyAction(n,e,i)}parseActions(n){let e=OF(n);if(!e){let i=n.getAttribute(Uu.JSACTION);if(!i)e=XF,sS(n,e);else{if(e=NF(i),!e){e={};let r=i.split(JF);for(let o=0;o<r.length;o++){let a=r[o];if(!a)continue;let s=a.indexOf(WF.EVENT_ACTION_SEPARATOR),l=s!==-1,c=l?a.substr(0,s).trim():eL,u=l?a.substr(s+1).trim():a;e[c]=u}PF(i,e)}sS(n,e)}}return e}addA11yClickSupport(n,e,i){this.a11yClickSupport=!0,this.updateEventInfoForA11yClick=n,this.preventDefaultForA11yClick=e,this.populateClickOnlyAction=i}},bS=(function(t){return t[t.I_AM_THE_JSACTION_FRAMEWORK=0]="I_AM_THE_JSACTION_FRAMEWORK",t})(bS||{}),N_=class{dispatchDelegate;actionResolver;eventReplayer;eventReplayScheduled=!1;replayEventInfoWrappers=[];constructor(n,{actionResolver:e,eventReplayer:i}={}){this.dispatchDelegate=n,this.actionResolver=e,this.eventReplayer=i}dispatch(n){let e=new k_(n);this.actionResolver?.resolveEventType(n),this.actionResolver?.resolveAction(n);let i=e.getAction();if(i&&tL(i.element,e)&&HF(e.getEvent()),this.eventReplayer&&e.getIsReplay()){this.scheduleEventInfoWrapperReplay(e);return}this.dispatchDelegate(e)}scheduleEventInfoWrapperReplay(n){this.replayEventInfoWrappers.push(n),!this.eventReplayScheduled&&(this.eventReplayScheduled=!0,Promise.resolve().then(()=>{this.eventReplayScheduled=!1,this.eventReplayer(this.replayEventInfoWrappers)}))}};function tL(t,n){return t.tagName==="A"&&(n.getEventType()===ae.CLICK||n.getEventType()===ae.CLICKMOD)}var CS=Symbol.for("propagationStopped"),V_={REPLAY:101};var nL="`preventDefault` called during event replay.";var iL="`composedPath` called during event replay.",Xf=class{dispatchDelegate;clickModSupport;actionResolver;dispatcher;constructor(n,e=!0){this.dispatchDelegate=n,this.clickModSupport=e,this.actionResolver=new O_({clickModSupport:e}),this.dispatcher=new N_(i=>{this.dispatchToDelegate(i)},{actionResolver:this.actionResolver})}dispatch(n){this.dispatcher.dispatch(n)}dispatchToDelegate(n){for(n.getIsReplay()&&aL(n),rL(n);n.getAction();){if(sL(n),F_(n.getEventType())&&n.getAction().element!==n.getTargetElement()||(this.dispatchDelegate(n.getEvent(),n.getAction().name),oL(n)))return;this.actionResolver.resolveParentAction(n.eventInfo)}}};function rL(t){let n=t.getEvent(),e=t.getEvent().stopPropagation.bind(n),i=()=>{n[CS]=!0,e()};Qo(n,"stopPropagation",i),Qo(n,"stopImmediatePropagation",i)}function oL(t){return!!t.getEvent()[CS]}function aL(t){let n=t.getEvent(),e=t.getTargetElement(),i=n.preventDefault.bind(n);Qo(n,"target",e),Qo(n,"eventPhase",V_.REPLAY),Qo(n,"preventDefault",()=>{throw i(),new Error(nL+"")}),Qo(n,"composedPath",()=>{throw new Error(iL+"")})}function sL(t){let n=t.getEvent(),e=t.getAction()?.element;e&&Qo(n,"currentTarget",e,{configurable:!0})}function Qo(t,n,e,{configurable:i=!1}={}){Object.defineProperty(t,n,{value:e,configurable:i})}function DS(t,n){t.ecrd(e=>{n.dispatch(e)},bS.I_AM_THE_JSACTION_FRAMEWORK)}function lL(t){return t?.q??[]}function cL(t){t&&(cS(t.c,t.et,t.h),cS(t.c,t.etc,t.h,!0))}function cS(t,n,e,i){for(let r=0;r<n.length;r++)t.removeEventListener(n[r],e,i)}var dL=!1,wS=(()=>{class t{static MOUSE_SPECIAL_SUPPORT=dL;containerManager;eventHandlers={};browserEventTypeToExtraEventTypes={};dispatcher=null;queuedEventInfos=[];constructor(e){this.containerManager=e}handleEvent(e,i,r){let o=KF(e,i,i.target,r,Date.now());this.handleEventInfo(o)}handleEventInfo(e){if(!this.dispatcher){gS(e,!0),this.queuedEventInfos?.push(e);return}this.dispatcher(e)}addEvent(e,i,r){if(e in this.eventHandlers||!this.containerManager||!t.MOUSE_SPECIAL_SUPPORT&&FF.indexOf(e)>=0)return;let o=(s,l,c)=>{this.handleEvent(s,l,c)};this.eventHandlers[e]=o;let a=VF(i||e);if(a!==e){let s=this.browserEventTypeToExtraEventTypes[a]||[];s.push(e),this.browserEventTypeToExtraEventTypes[a]=s}this.containerManager.addEventListener(a,s=>l=>{o(e,l,s)},r)}replayEarlyEvents(e=window._ejsa){e&&(this.replayEarlyEventInfos(e.q),cL(e),delete window._ejsa)}replayEarlyEventInfos(e){for(let i=0;i<e.length;i++){let r=e[i],o=this.getEventTypesForBrowserEventType(r.eventType);for(let a=0;a<o.length;a++){let s=yS(r);L_(s,o[a]),this.handleEventInfo(s)}}}getEventTypesForBrowserEventType(e){let i=[];return this.eventHandlers[e]&&i.push(e),this.browserEventTypeToExtraEventTypes[e]&&i.push(...this.browserEventTypeToExtraEventTypes[e]),i}handler(e){return this.eventHandlers[e]}cleanUp(){this.containerManager?.cleanUp(),this.containerManager=null,this.eventHandlers={},this.browserEventTypeToExtraEventTypes={},this.dispatcher=null,this.queuedEventInfos=[]}registerDispatcher(e,i){this.ecrd(e,i)}ecrd(e,i){if(this.dispatcher=e,this.queuedEventInfos?.length){for(let r=0;r<this.queuedEventInfos.length;r++)this.handleEventInfo(this.queuedEventInfos[r]);this.queuedEventInfos=null}}}return t})();function ES(t,n=window){return lL(n._ejsas?.[t])}function j_(t,n=window){n._ejsas&&(n._ejsas[t]=void 0)}var PS=Symbol("InputSignalNode#UNSET"),xL=se(D({},pl),{transformFn:void 0,applyValueToInputSignal(t,n){Do(t,n)}});function FS(t,n){let e=Object.create(xL);e.value=t,e.transformFn=n?.transform;function i(){if(Mr(e),e.value===PS){let r=null;throw new k(-950,r)}return e.value}return i[ht]=e,i}var an=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>tc(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function xS(t,n){return FS(t,n)}function SL(t){return FS(PS,t)}var np=(xS.required=SL,xS);function SS(t,n){return h_(n)}function IL(t,n){return m_(n)}var mc=(SS.required=IL,SS);function IS(t,n){return h_(n)}function ML(t,n){return m_(n)}var LS=(IS.required=ML,IS);var H_=new y(""),TL=new y("");function hc(t){return!t.moduleRef}function AL(t){let n=hc(t)?t.r3Injector:t.moduleRef.injector,e=n.get(K);return e.run(()=>{hc(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(vn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),hc(t)){let o=()=>n.destroy(),a=t.platformInjector.get(H_);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(H_);a.add(o),t.moduleRef.onDestroy(()=>{Gl(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return kL(i,e,()=>{let o=n.get(Ri),a=o.add(),s=n.get(D_);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(pc,fc);if(Hx(l||fc),!n.get(TL,!0))return hc(t)?n.get(Et):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(hc(t)){let u=n.get(Et);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return RL?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var RL;function kL(t,n,e){try{let i=e();return qr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var ep=null;function OL(t=[],n){return J.create({name:n,providers:[{provide:Pl,useValue:"platform"},{provide:H_,useValue:new Set([()=>ep=null])},...t]})}function NL(t=[]){if(ep)return ep;let n=OL(t);return ep=n,Nx(),PL(n),n}function PL(t){let n=t.get(yf,null);kt(t,()=>{n?.forEach(e=>e())})}var Jf=new WeakSet,MS="";function TS(t){return t.get(Mv,Fw)}function VS(){let t=[{provide:Mv,useFactory:()=>{let n=!0;{let e=d(kn);n=!!window._ejsas?.[e]}return n&&On("NgEventReplay"),n}}];return t.push({provide:Ei,useValue:()=>{let n=d(Et),{injector:e}=n;if(!Jf.has(n)){let i=d(Tv);if(TS(e)){Gw();let r=e.get(kn),o=zw(r,(a,s,l)=>{a.nodeType===Node.ELEMENT_NODE&&(jw(a,s,l),Bw(a,i))});n.onDestroy(o)}}},multi:!0},{provide:Yr,useFactory:()=>{let n=d(Et),{injector:e}=n;return()=>{if(!TS(e)||Jf.has(n))return;Jf.add(n);let i=e.get(kn);n.onDestroy(()=>{Jf.delete(n),j_(i)}),n.whenStable().then(()=>{if(n.destroyed)return;let r=e.get(Uw);FL(r,e);let o=e.get(Tv);o.get(MS)?.forEach(Hw),o.delete(MS);let a=r.instance;Xw(e)?n.onDestroy(()=>a.cleanUp()):a.cleanUp()})}},multi:!0}),t}var FL=(t,n)=>{let e=n.get(kn),i=window._ejsas[e],r=t.instance=new wS(new Kf(i.c));for(let s of i.et)r.addEvent(s);for(let s of i.etc)r.addEvent(s);let o=ES(e);r.replayEarlyEventInfos(o),j_(e);let a=new Xf(s=>{LL(n,s,s.currentTarget)});DS(r,a)};function LL(t,n,e){let i=(e&&e.getAttribute(ic))??"";/d\d+/.test(i)?VL(i,t,n,e):n.eventPhase===V_.REPLAY&&Av(n,e)}function VL(t,n,e,i){let r=n.get(Lw);r.push({event:e,currentTarget:i}),Lx(n,t,jL(r))}function jL(t){return n=>{let e=new Set(n),i=[];for(let{event:r,currentTarget:o}of t){let a=o.getAttribute(ic);e.has(a)?Av(r,o):i.push({event:r,currentTarget:o})}t.length=0,t.push(...i)}}var AS=!1;var BL=1e4;function HL(){AS||(AS=!0,Qw(),Vx(),Kx(),Bx(),Ax(),mx(),XE(),OE())}function UL(t){return t.whenStable()}function jS(){let t=[{provide:es,useFactory:()=>{let n=!0;return n=!!d(Ja,{optional:!0})?.get(Rv,null),n&&On("NgHydration"),n}},{provide:Ei,useValue:()=>{YE(!1);let n=d(j);d(es)&&(tE(n),HL())},multi:!0}];return t.push({provide:Iv,useFactory:()=>d(es)},{provide:Yr,useFactory:()=>{let n=d(Qn);if(d(es)){let e=d(Et);return()=>{UL(e).then(()=>{e.destroyed||(a_(e),n.notify(7))})}}return()=>{}},multi:!0}),xi(t)}var qX=BL-1e3;var Re=(()=>{class t{static __NG_ELEMENT_ID__=zL}return t})();function zL(t){return $L(Mt(),re(),(t&16)===16)}function $L(t,n,e){if(Mi(t)&&!e){let i=Rn(t.index,n);return new Gr(i,i)}else if(t.type&175){let i=n[Nt];return new Gr(i,n)}return null}var U_=class{supports(n){return c_(n)}create(n){return new z_(n)}},GL=(t,n)=>n,z_=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||GL}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let a=!i||e&&e.currentIndex<RS(i,r,o)?e:i,s=RS(a,r,o),l=a.currentIndex;if(a===i)r--,i=i._nextRemoved;else if(e=e._next,a.previousIndex==null)r++;else{o||(o=[]);let c=s-r,u=l-r;if(c!=u){for(let p=0;p<c;p++){let h=p<o.length?o[p]:o[p]=0,_=h+p;u<=_&&_<c&&(o[p]=h+1)}let f=a.previousIndex;o[f]=u-c}}s!==l&&n(a,s,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!c_(n))throw new k(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,a;if(Array.isArray(n)){this.length=n.length;for(let s=0;s<this.length;s++)o=n[s],a=this._trackByFn(s,o),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,o,a,s),i=!0):(i&&(e=this._verifyReinsertion(e,o,a,s)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,ox(n,s=>{a=this._trackByFn(r,s),e===null||!Object.is(e.trackById,a)?(e=this._mismatch(e,s,a,r),i=!0):(i&&(e=this._verifyReinsertion(e,s,a,r)),Object.is(e.item,s)||this._addIdentityChange(e,s)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new $_(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new tp),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new tp),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},$_=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},G_=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},tp=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new G_,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function RS(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function kS(){return new ar([new U_])}var ar=(()=>{class t{factories;static \u0275prov=w({token:t,providedIn:"root",factory:kS});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||kS())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new k(901,!1)}}return t})();function BS(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Fe(Te.BootstrapApplicationStart);try{let o=r?.injector??NL(i),a=[oS(),aD,...e||[]],s=new Xl({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return AL({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Fe(Te.BootstrapApplicationEnd)}}function B(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function si(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var B_=Symbol("NOT_SET"),HS=new Set,WL=se(D({},pl),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:B_,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==B_&&!wa(this))return this.signal;try{for(let r of this.cleanup??HS)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Zi(this),i;try{i=this.userFn.apply(null,n)}finally{Tr(this,e)}return(this.value===B_||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),W_=class extends Yl{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Kt),a),this.scheduler=r;for(let s of Yv){let l=e[s];if(l===void 0)continue;let c=Object.create(WL);c.sequence=this,c.phase=s,c.userFn=l,c.dirty=!0,c.signal=()=>(Mr(c),c.value),c.signal[ht]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[s]=c,this.hooks[s]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??HS)e()}finally{Ar(n)}}};function US(t,n){let e=n?.injector??d(J),i=e.get(Qn),r=e.get(If),o=e.get(ai,null,{optional:!0});r.impl??=e.get(Qv);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(Ua,null,{optional:!0}),l=new W_(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function ip(t,n){let e=Zn(t),i=n.elementInjector||Fa();return new Wr(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}function zS(t){let n=Zn(t);if(!n)return null;let e=new Wr(n);return{get selector(){return e.selector},get type(){return e.componentType},get inputs(){return e.inputs},get outputs(){return e.outputs},get ngContentSelectors(){return e.ngContentSelectors},get isStandalone(){return n.standalone},get isSignal(){return n.signals}}}var $S=null;function Nn(){return $S}function q_(t){$S??=t}var gc=class{},rp=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(GS),providedIn:"platform"})}return t})();var GS=(()=>{class t extends rp{_location;_history;_doc=d(j);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Nn().getBaseHref(this._doc)}onPopState(e){let i=Nn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Nn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function YS(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function WS(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Kr(t){return t&&t[0]!=="?"?`?${t}`:t}var us=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(YL),providedIn:"root"})}return t})(),qL=new y(""),YL=(()=>{class t extends us{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(j).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return YS(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Kr(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+Kr(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+Kr(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(ne(rp),ne(qL,8))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Xr=(()=>{class t{_subject=new S;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=KL(WS(qS(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Kr(i))}normalize(e){return t.stripTrailingSlash(ZL(this._basePath,qS(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Kr(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Kr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Kr;static joinWithSlash=YS;static stripTrailingSlash=WS;static \u0275fac=function(i){return new(i||t)(ne(us))};static \u0275prov=w({token:t,factory:()=>QL(),providedIn:"root"})}return t})();function QL(){return new Xr(ne(us))}function ZL(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function qS(t){return t.replace(/\/index.html$/,"")}function KL(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var KS={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},Z_=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(Z_||{});var ji={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function fs(t,n){let e=uc(t),i=e[Qr.NumberSymbols][n];if(typeof i>"u"){if(n===ji.CurrencyDecimal)return e[Qr.NumberSymbols][ji.Decimal];if(n===ji.CurrencyGroup)return e[Qr.NumberSymbols][ji.Group]}return i}function XS(t,n){return uc(t)[Qr.NumberFormats][n]}function XL(t){return uc(t)[Qr.Currencies]}function JS(t,n,e="en"){let i=XL(e)[t]||KS[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var JL=2;function eI(t){let n,e=KS[t];return e&&(n=e[2]),typeof n=="number"?n:JL}var e2=/^(\d+)?\.((\d+)(-(\d+))?)?$/,QS=22,op=".",vc="0",t2=";",n2=",",Y_="#",ZS="\xA4";function i2(t,n,e,i,r,o,a=!1){let s="",l=!1;if(!isFinite(t))s=fs(e,ji.Infinity);else{let c=a2(t);a&&(c=o2(c));let u=n.minInt,f=n.minFrac,p=n.maxFrac;if(o){let pe=o.match(e2);if(pe===null)throw new k(2306,!1);let pt=pe[1],ct=pe[3],Sr=pe[5];pt!=null&&(u=Q_(pt)),ct!=null&&(f=Q_(ct)),Sr!=null?p=Q_(Sr):ct!=null&&f>p&&(p=f);let Qi=100;if(u>Qi||f>Qi||p>Qi)throw new k(2306,!1)}s2(c,f,p);let h=c.digits,_=c.integerLen,x=c.exponent,T=[];for(l=h.every(pe=>!pe);_<u;_++)h.unshift(0);for(;_<0;_++)h.unshift(0);_>0?T=h.splice(_,h.length):(T=h,h=[0]);let N=[];for(h.length>=n.lgSize&&N.unshift(h.splice(-n.lgSize,h.length).join(""));h.length>n.gSize;)N.unshift(h.splice(-n.gSize,h.length).join(""));h.length&&N.unshift(h.join("")),s=N.join(fs(e,i)),T.length&&(s+=fs(e,r)+T.join("")),x&&(s+=fs(e,ji.Exponential)+"+"+x)}return t<0&&!l?s=n.negPre+s+n.negSuf:s=n.posPre+s+n.posSuf,s}function tI(t,n,e,i,r){let o=XS(n,Z_.Currency),a=r2(o,fs(n,ji.MinusSign));return a.minFrac=eI(i),a.maxFrac=a.minFrac,i2(t,a,n,ji.CurrencyGroup,ji.CurrencyDecimal,r).replace(ZS,e).replace(ZS,"").trim()}function r2(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(t2),r=i[0],o=i[1],a=r.indexOf(op)!==-1?r.split(op):[r.substring(0,r.lastIndexOf(vc)+1),r.substring(r.lastIndexOf(vc)+1)],s=a[0],l=a[1]||"";e.posPre=s.substring(0,s.indexOf(Y_));for(let u=0;u<l.length;u++){let f=l.charAt(u);f===vc?e.minFrac=e.maxFrac=u+1:f===Y_?e.maxFrac=u+1:e.posSuf+=f}let c=s.split(n2);if(e.gSize=c[1]?c[1].length:0,e.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,f=o.indexOf(Y_);e.negPre=o.substring(0,f).replace(/'/g,""),e.negSuf=o.slice(f+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function o2(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function a2(t){let n=Math.abs(t)+"",e=0,i,r,o,a,s;for((r=n.indexOf(op))>-1&&(n=n.replace(op,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===vc;o++);if(o===(s=n.length))i=[0],r=1;else{for(s--;n.charAt(s)===vc;)s--;for(r-=o,i=[],a=0;o<=s;o++,a++)i[a]=Number(n.charAt(o))}return r>QS&&(i=i.splice(0,QS-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function s2(t,n,e){if(n>e)throw new k(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),a=o+t.integerLen,s=i[a];if(a>0){i.splice(Math.max(t.integerLen,a));for(let f=a;f<i.length;f++)i[f]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,a=o+1),i[0]=0;for(let f=1;f<a;f++)i[f]=0}if(s>=5)if(a-1<0){for(let f=0;f>a;f--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[a-1]++;for(;r<Math.max(0,o);r++)i.push(0);let l=o!==0,c=n+t.integerLen,u=i.reduceRight(function(f,p,h,_){return p=p+f,_[h]=p<10?p:p-10,l&&(_[h]===0&&h>=c?_.pop():l=!1),p>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function Q_(t){let n=parseInt(t);if(isNaN(n))throw new k(2305,!1);return n}var ps=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(J);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ee(Je))};static \u0275dir=E({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ae]})}return t})();function l2(t,n){return new k(2100,!1)}var Jr=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,a){if(!c2(e))return null;a||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let s=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?s=JS(s,r==="symbol"?"wide":"narrow",a):s=r);try{let l=d2(e);return tI(l,a,s,i,o)}catch(l){throw l2(t,l.message)}}static \u0275fac=function(i){return new(i||t)(ee(pc,16),ee(A_,16))};static \u0275pipe=g_({name:"currency",type:t,pure:!0})}return t})();function c2(t){return!(t==null||t===""||t!==t)}function d2(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new k(2309,!1);return t}var hs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})();function ap(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Zo=class{};var K_="browser";function sp(t){return t===K_}var _c=class{_doc;constructor(n){this._doc=n}manager},cp=(()=>{class t extends _c{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(ne(j))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),fp=new y(""),ty=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof cp));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof cp);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new k(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(ne(fp),ne(K))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),X_="ng-app-id";function nI(t){for(let n of t)n.remove()}function iI(t,n){let e=n.createElement("style");return e.textContent=t,e}function f2(t,n,e,i){let r=t.head?.querySelectorAll(`style[${X_}="${n}"],link[${X_}="${n}"]`);if(r)for(let o of r)o.removeAttribute(X_),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function ey(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var ny=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,f2(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,iI);i?.forEach(r=>this.addUsage(r,this.external,ey))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(nI(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])nI(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,iI(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,ey(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(ne(j),ne(kn),ne(Yo,8),ne(ir))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),J_={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},iy=/%COMP%/g;var oI="%COMP%",p2=`_nghost-${oI}`,h2=`_ngcontent-${oI}`,m2=!0,g2=new y("",{factory:()=>m2});function v2(t){return h2.replace(iy,t)}function _2(t){return p2.replace(iy,t)}function aI(t,n){return n.map(e=>e.replace(iy,t))}var ry=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,a,s,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.defaultRenderer=new yc(e,a,s,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof up?r.applyToHost(e):r instanceof bc&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case ri.Emulated:o=new up(l,c,i,this.appId,u,a,s,f);break;case ri.ShadowDom:return new dp(l,e,i,a,s,this.nonce,f,c);case ri.ExperimentalIsolatedShadowDom:return new dp(l,e,i,a,s,this.nonce,f);default:o=new bc(l,c,i,u,a,s,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(ne(ty),ne(ny),ne(kn),ne(g2),ne(j),ne(K),ne(Yo),ne(ai,8))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),yc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(J_[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(rI(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(rI(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new k(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=J_[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=J_[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Oi.DashCase|Oi.Important)?n.style.setProperty(e,i,r&Oi.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Oi.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Nn().getGlobalEventTarget(this.doc,n),!n))throw new k(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function rI(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var dp=class extends yc{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l){super(n,r,o,s),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=aI(i.id,c);for(let f of c){let p=document.createElement("style");a&&p.setAttribute("nonce",a),p.textContent=f,this.shadowRoot.appendChild(p)}let u=i.getExternalStyles?.();if(u)for(let f of u){let p=ey(f,r);a&&p.setAttribute("nonce",a),this.shadowRoot.appendChild(p)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},bc=class extends yc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l){super(n,o,a,s),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?aI(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Go.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},up=class extends bc{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l){let c=r+"-"+i.id;super(n,e,i,o,a,s,l,c),this.contentAttr=v2(c),this.hostAttr=_2(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var pp=class t extends gc{supportsDOMEvents=!0;static makeCurrent(){q_(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=y2();return e==null?null:b2(e)}resetBaseElement(){Cc=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return ap(document.cookie,n)}},Cc=null;function y2(){return Cc=Cc||document.head.querySelector("base"),Cc?Cc.getAttribute("href"):null}function b2(t){return new URL(t,document.baseURI).pathname}var C2=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),sI=["alt","control","meta","shift"],D2={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},w2={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},lI=(()=>{class t extends _c{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Nn().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),sI.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=D2[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),sI.forEach(a=>{if(a!==r){let s=w2[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(ne(j))};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();async function oy(t,n,e){let i=D({rootComponent:t},E2(n,e));return BS(i)}function E2(t,n){return{platformRef:n?.platformRef,appProviders:[...T2,...t?.providers??[]],platformProviders:M2}}function x2(){pp.makeCurrent()}function S2(){return new qt}function I2(){return Ev(document),document}var M2=[{provide:ir,useValue:K_},{provide:yf,useValue:x2,multi:!0},{provide:j,useFactory:I2}];var T2=[{provide:Pl,useValue:"root"},{provide:qt,useFactory:S2},{provide:fp,useClass:cp,multi:!0},{provide:fp,useClass:lI,multi:!0},ry,ny,ty,{provide:wt,useExisting:ry},{provide:Zo,useClass:C2},[]];var lr=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=this.headers.get(e);if(!a)return;a=a.filter(s=>o.indexOf(s)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var sy=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},ly=class{encodeKey(n){return cI(n)}encodeValue(n){return cI(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function A2(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(a)||[];l.push(s),e.set(a,l)}),e}var R2=/%(\d[a-f0-9])/gi,k2={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function cI(t){return encodeURIComponent(t).replace(R2,(n,e)=>k2[e]??n)}function hp(t){return`${t}`}var sr=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new ly,n.fromString){if(n.fromObject)throw new k(2805,!1);this.map=A2(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(hp):[hp(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(hp(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(hp(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function O2(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function dI(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function uI(t){return typeof Blob<"u"&&t instanceof Blob}function fI(t){return typeof FormData<"u"&&t instanceof FormData}function N2(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var pI="Content-Type",hI="Accept",mI="text/plain",gI="application/json",P2=`${gI}, ${mI}, */*`,ms=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(O2(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new k(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new lr,this.context??=new sy,!this.params)this.params=new sr,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e.indexOf("?"),l=s===-1?"?":s<e.length-1?"&":"";this.urlWithParams=e+l+a}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||dI(this.body)||uI(this.body)||fI(this.body)||N2(this.body)?this.body:this.body instanceof sr?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||fI(this.body)?null:uI(this.body)?this.body.type||null:dI(this.body)?null:typeof this.body=="string"?mI:this.body instanceof sr?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?gI:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer||this.referrer,p=n.integrity||this.integrity,h=n.referrerPolicy||this.referrerPolicy,_=n.transferCache??this.transferCache,x=n.timeout??this.timeout,T=n.body!==void 0?n.body:this.body,N=n.withCredentials??this.withCredentials,pe=n.reportProgress??this.reportProgress,pt=n.headers||this.headers,ct=n.params||this.params,Sr=n.context??this.context;return n.setHeaders!==void 0&&(pt=Object.keys(n.setHeaders).reduce((Qi,_o)=>Qi.set(_o,n.setHeaders[_o]),pt)),n.setParams&&(ct=Object.keys(n.setParams).reduce((Qi,_o)=>Qi.set(_o,n.setParams[_o]),ct)),new t(e,i,T,{params:ct,headers:pt,context:Sr,reportProgress:pe,responseType:r,withCredentials:N,transferCache:_,keepalive:o,cache:s,priority:a,timeout:x,mode:l,redirect:c,credentials:u,referrer:f,integrity:p,referrerPolicy:h})}},Ko=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Ko||{}),Dc=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new lr,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},cy=class t extends Dc{constructor(n={}){super(n)}type=Ko.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},vs=class t extends Dc{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Ko.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},gs=class extends Dc{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},F2=200,L2=204;var V2=/^\)\]\}',?\n/;var j2=(()=>{class t{xhrFactory;tracingService=d(ai,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new k(-2800,!1);let i=this.xhrFactory;return Q(null).pipe(at(()=>new he(o=>{let a=i.build();if(a.open(e.method,e.urlWithParams),e.withCredentials&&(a.withCredentials=!0),e.headers.forEach((T,N)=>a.setRequestHeader(T,N.join(","))),e.headers.has(hI)||a.setRequestHeader(hI,P2),!e.headers.has(pI)){let T=e.detectContentTypeHeader();T!==null&&a.setRequestHeader(pI,T)}if(e.timeout&&(a.timeout=e.timeout),e.responseType){let T=e.responseType.toLowerCase();a.responseType=T!=="json"?T:"text"}let s=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let T=a.statusText||"OK",N=new lr(a.getAllResponseHeaders()),pe=a.responseURL||e.url;return l=new cy({headers:N,status:a.status,statusText:T,url:pe}),l},u=this.maybePropagateTrace(()=>{let{headers:T,status:N,statusText:pe,url:pt}=c(),ct=null;N!==L2&&(ct=typeof a.response>"u"?a.responseText:a.response),N===0&&(N=ct?F2:0);let Sr=N>=200&&N<300;if(e.responseType==="json"&&typeof ct=="string"){let Qi=ct;ct=ct.replace(V2,"");try{ct=ct!==""?JSON.parse(ct):null}catch(_o){ct=Qi,Sr&&(Sr=!1,ct={error:_o,text:ct})}}Sr?(o.next(new vs({body:ct,headers:T,status:N,statusText:pe,url:pt||void 0})),o.complete()):o.error(new gs({error:ct,headers:T,status:N,statusText:pe,url:pt||void 0}))}),f=this.maybePropagateTrace(T=>{let{url:N}=c(),pe=new gs({error:T,status:a.status||0,statusText:a.statusText||"Unknown Error",url:N||void 0});o.error(pe)}),p=f;e.timeout&&(p=this.maybePropagateTrace(T=>{let{url:N}=c(),pe=new gs({error:new DOMException("Request timed out","TimeoutError"),status:a.status||0,statusText:a.statusText||"Request timeout",url:N||void 0});o.error(pe)}));let h=!1,_=this.maybePropagateTrace(T=>{h||(o.next(c()),h=!0);let N={type:Ko.DownloadProgress,loaded:T.loaded};T.lengthComputable&&(N.total=T.total),e.responseType==="text"&&a.responseText&&(N.partialText=a.responseText),o.next(N)}),x=this.maybePropagateTrace(T=>{let N={type:Ko.UploadProgress,loaded:T.loaded};T.lengthComputable&&(N.total=T.total),o.next(N)});return a.addEventListener("load",u),a.addEventListener("error",f),a.addEventListener("timeout",p),a.addEventListener("abort",f),e.reportProgress&&(a.addEventListener("progress",_),s!==null&&a.upload&&a.upload.addEventListener("progress",x)),a.send(s),o.next({type:Ko.Sent}),()=>{a.removeEventListener("error",f),a.removeEventListener("abort",f),a.removeEventListener("load",u),a.removeEventListener("timeout",p),e.reportProgress&&(a.removeEventListener("progress",_),s!==null&&a.upload&&a.upload.removeEventListener("progress",x)),a.readyState!==a.DONE&&a.abort()}})))}static \u0275fac=function(i){return new(i||t)(ne(Zo))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function B2(t,n){return n(t)}function H2(t,n,e){return(i,r)=>kt(e,()=>n(i,o=>t(o,r)))}var U2=new y("",{factory:()=>[]}),dy=new y(""),z2=new y("",{factory:()=>!0});var $2=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(j2),r},providedIn:"root"})}return t})();var G2=(()=>{class t{backend;injector;chain=null;pendingTasks=d(za);contributeToStability=d(z2);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(U2),...this.injector.get(dy,[])]));this.chain=i.reduceRight((r,o)=>H2(r,o,this.injector),B2)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Ao(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(ne($2),ne(ze))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),W2=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(G2),r},providedIn:"root"})}return t})();function ay(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var uy=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof ms)o=e;else{let l;r.headers instanceof lr?l=r.headers:l=new lr(r.headers);let c;r.params&&(r.params instanceof sr?c=r.params:c=new sr({fromObject:r.params})),o=new ms(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=Q(o).pipe(To(l=>this.handler.handle(l)));if(e instanceof ms||r.observe==="events")return a;let s=a.pipe(Ee(l=>l instanceof vs));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(ce(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new k(2806,!1);return l.body}));case"blob":return s.pipe(ce(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new k(2807,!1);return l.body}));case"text":return s.pipe(ce(l=>{if(l.body!==null&&typeof l.body!="string")throw new k(2808,!1);return l.body}));default:return s.pipe(ce(l=>l.body))}case"response":return s;default:throw new k(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new sr().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,ay(r,i))}post(e,i,r={}){return this.request("POST",e,ay(r,i))}put(e,i,r={}){return this.request("PUT",e,ay(r,i))}static \u0275fac=function(i){return new(i||t)(ne(W2))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var q2=new y(""),Y2="b",Q2="h",Z2="s",K2="st",X2="u",J2="rt",fy=new y(""),eV=["GET","HEAD"];function yI(t,n){let a=n,{isCacheActive:e}=a,i=S0(a,["isCacheActive"]),{transferCache:r,method:o}=t;return!(!e||r===!1||t.withCredentials||o==="POST"&&!i.includePostRequests&&!r||o!=="POST"&&!eV.includes(o)||!i.includeRequestsWithAuthHeaders&&iV(t)||i.filter?.(t)===!1)}function bI(t,n){let{includeHeaders:e}=t,i=e;return typeof n=="object"&&n.includeHeaders&&(i=n.includeHeaders),i}function tV(t,n,e,i){let{transferCache:r}=t;if(!yI(t,n))return null;if(i)throw new k(2803,!1);let o=t.url,a=CI(t,o),s=e.get(a,null),l=bI(n,r);if(s){let{[Y2]:c,[J2]:u,[Q2]:f,[Z2]:p,[K2]:h,[X2]:_}=s,x=c;switch(u){case"arraybuffer":x=_I(c);break;case"blob":x=new Blob([_I(c)]);break}let T=new lr(f);return new vs({body:x,headers:T,status:p,statusText:h,url:_})}return null}function nV(t,n){let e=d(fy),i=d(Ja),r=d(q2,{optional:!0}),o=tV(t,e,i,r);if(o)return Q(o);let{transferCache:a}=t,s=bI(e,a),l=t.url,c=CI(t,l);return yI(t,e),n(t)}function iV(t){return t.headers.has("authorization")||t.headers.has("proxy-authorization")||t.headers.has("cookie")}function vI(t){return[...t.keys()].sort().map(n=>`${n}=${t.getAll(n)}`).join("&")}function CI(t,n){let{params:e,method:i,responseType:r}=t,o=vI(e),a=t.serializeBody();a instanceof URLSearchParams?a=vI(a):typeof a!="string"&&(a="");let s=[i,r,n,a,o].join("|"),l=rV(s);return l}function rV(t){let n=0;for(let e of t)n=Math.imul(31,n)+e.charCodeAt(0)<<0;return n+=2147483648,n.toString()}function _I(t){let n=atob(t);return Uint8Array.from(n,i=>i.charCodeAt(0)).buffer}function DI(t){return[{provide:fy,useFactory:()=>(On("NgHttpTransferCache"),D({isCacheActive:!0},t))},{provide:dy,useValue:nV,multi:!0},{provide:Yr,multi:!0,useFactory:()=>{let n=d(Et),e=d(fy);return()=>{n.whenStable().then(()=>{e.isCacheActive=!1})}}}]}var wI=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(ne(j))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=ne(aV),r},providedIn:"root"})}return t})(),aV=(()=>{class t extends wc{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Tt.NONE:return i;case Tt.HTML:return rr(i,"HTML")?oi(i):jv(this._doc,String(i)).toString();case Tt.STYLE:return rr(i,"Style")?oi(i):i;case Tt.SCRIPT:if(rr(i,"Script"))return oi(i);throw new k(5200,!1);case Tt.URL:return rr(i,"URL")?oi(i):oc(String(i));case Tt.RESOURCE_URL:if(rr(i,"ResourceURL"))return oi(i);throw new k(5201,!1);default:throw new k(5202,!1)}}bypassSecurityTrustHtml(e){return Nv(e)}bypassSecurityTrustStyle(e){return Pv(e)}bypassSecurityTrustScript(e){return Fv(e)}bypassSecurityTrustUrl(e){return Lv(e)}bypassSecurityTrustResourceUrl(e){return Vv(e)}static \u0275fac=function(i){return new(i||t)(ne(j))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),mp=(function(t){return t[t.NoHttpTransferCache=0]="NoHttpTransferCache",t[t.HttpTransferCacheOptions=1]="HttpTransferCacheOptions",t[t.I18nSupport=2]="I18nSupport",t[t.EventReplay=3]="EventReplay",t[t.IncrementalHydration=4]="IncrementalHydration",t})(mp||{});function sV(t,n=[],e={}){return{\u0275kind:t,\u0275providers:n}}function EI(){return sV(mp.EventReplay,VS())}function xI(...t){let n=[],e=new Set;for(let{\u0275providers:r,\u0275kind:o}of t)e.add(o),r.length&&n.push(r);let i=e.has(mp.HttpTransferCacheOptions);return xi([[],[],jS(),e.has(mp.NoHttpTransferCache)||i?[]:DI({}),n])}var ye="primary",Vc=Symbol("RouteTitle"),vy=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Jo(t){return new vy(t)}function py(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function OI(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return py(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!py(o,t.slice(0,o.length),s)||!py(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function Cp(t){return new Promise((n,e)=>{t.pipe(Xi()).subscribe({next:i=>n(i),error:i=>e(i)})})}function lV(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Bi(t[e],n[e]))return!1;return!0}function Bi(t,n){let e=t?_y(t):void 0,i=n?_y(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!NI(t[r],n[r]))return!1;return!0}function _y(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function NI(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function cV(t){return t.length>0?t[t.length-1]:null}function na(t){return Io(t)?t:qr(t)?Ke(Promise.resolve(t)):Q(t)}function PI(t){return Io(t)?Cp(t):Promise.resolve(t)}var dV={exact:LI,subset:VI},FI={exact:uV,subset:fV,ignored:()=>!0},ky={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Tc={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function Oy(t,n,e){let i=t instanceof Jt?t:n.parseUrl(t);return xt(()=>yy(n.lastSuccessfulNavigation()?.finalUrl??new Jt,i,D(D({},Tc),e)))}function yy(t,n,e){return dV[e.paths](t.root,n.root,e.matrixParams)&&FI[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function uV(t,n){return Bi(t,n)}function LI(t,n,e){if(!Xo(t.segments,n.segments)||!_p(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!LI(t.children[i],n.children[i],e))return!1;return!0}function fV(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>NI(t[e],n[e]))}function VI(t,n,e){return jI(t,n,n.segments,e)}function jI(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Xo(r,e)||n.hasChildren()||!_p(r,e,i))}else if(t.segments.length===e.length){if(!Xo(t.segments,e)||!_p(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!VI(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Xo(t.segments,r)||!_p(t.segments,r,i)||!t.children[ye]?!1:jI(t.children[ye],n,o,i)}}function _p(t,n,e){return n.every((i,r)=>FI[e](t[r].parameters,i.parameters))}var Jt=class{root;queryParams;fragment;_queryParamMap;constructor(n=new He([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Jo(this.queryParams),this._queryParamMap}toString(){return mV.serialize(this)}},He=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return yp(this)}},eo=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Jo(this.parameters),this._parameterMap}toString(){return HI(this)}};function pV(t,n){return Xo(t,n)&&t.every((e,i)=>Bi(e.parameters,n[i].parameters))}function Xo(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function hV(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===ye&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==ye&&(e=e.concat(n(r,i)))}),e}var Ss=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new to,providedIn:"root"})}return t})(),to=class{parse(n){let e=new Cy(n);return new Jt(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Ec(n.root,!0)}`,i=_V(n.queryParams),r=typeof n.fragment=="string"?`#${gV(n.fragment)}`:"";return`${e}${i}${r}`}},mV=new to;function yp(t){return t.segments.map(n=>HI(n)).join("/")}function Ec(t,n){if(!t.hasChildren())return yp(t);if(n){let e=t.children[ye]?Ec(t.children[ye],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==ye&&i.push(`${r}:${Ec(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=hV(t,(i,r)=>r===ye?[Ec(t.children[ye],!1)]:[`${r}:${Ec(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[ye]!=null?`${yp(t)}/${e[0]}`:`${yp(t)}/(${e.join("//")})`}}function BI(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function gp(t){return BI(t).replace(/%3B/gi,";")}function gV(t){return encodeURI(t)}function by(t){return BI(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function bp(t){return decodeURIComponent(t)}function II(t){return bp(t.replace(/\+/g,"%20"))}function HI(t){return`${by(t.path)}${vV(t.parameters)}`}function vV(t){return Object.entries(t).map(([n,e])=>`;${by(n)}=${by(e)}`).join("")}function _V(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${gp(e)}=${gp(r)}`).join("&"):`${gp(e)}=${gp(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var yV=/^[^\/()?;#]+/;function hy(t){let n=t.match(yV);return n?n[0]:""}var bV=/^[^\/()?;=#]+/;function CV(t){let n=t.match(bV);return n?n[0]:""}var DV=/^[^=?&#]+/;function wV(t){let n=t.match(DV);return n?n[0]:""}var EV=/^[^&#]+/;function xV(t){let n=t.match(EV);return n?n[0]:""}var Cy=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new He([],{}):new He([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new k(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[ye]=new He(e,i)),r}parseSegment(){let n=hy(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new k(4009,!1);return this.capture(n),new eo(bp(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=CV(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=hy(this.remaining);r&&(i=r,this.capture(i))}n[bp(e)]=bp(i)}parseQueryParam(n){let e=wV(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=xV(this.remaining);a&&(i=a,this.capture(i))}let r=II(e),o=II(i);if(n.hasOwnProperty(r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=hy(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new k(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=ye);let s=this.parseChildren(e+1);i[a??ye]=Object.keys(s).length===1&&s[ye]?s[ye]:new He([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new k(4011,!1)}};function UI(t){return t.segments.length>0?new He([],{[ye]:t}):t}function zI(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=zI(r);if(i===ye&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new He(t.segments,n);return SV(e)}function SV(t){if(t.numberOfChildren===1&&t.children[ye]){let n=t.children[ye];return new He(t.segments.concat(n.segments),n.children)}return t}function no(t){return t instanceof Jt}function $I(t,n,e=null,i=null,r=new to){let o=GI(t);return WI(o,n,e,i,r)}function GI(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new He(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=UI(i);return n??r}function WI(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return my(o,o,o,e,i,r);let a=IV(n);if(a.toRoot())return my(o,o,new He([],{}),e,i,r);let s=MV(a,o,t),l=s.processChildren?Sc(s.segmentGroup,s.index,a.commands):YI(s.segmentGroup,s.index,a.commands);return my(o,s.segmentGroup,l,e,i,r)}function Dp(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Ac(t){return typeof t=="object"&&t!=null&&t.outlets}function MI(t,n,e){t||="\u0275";let i=new Jt;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function my(t,n,e,i,r,o){let a={};for(let[c,u]of Object.entries(i??{}))a[c]=Array.isArray(u)?u.map(f=>MI(c,f,o)):MI(c,u,o);let s;t===n?s=e:s=qI(t,n,e);let l=UI(zI(s));return new Jt(l,a,r)}function qI(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=qI(o,n,e)}),new He(t.segments,i)}var wp=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Dp(i[0]))throw new k(4003,!1);let r=i.find(Ac);if(r&&r!==cV(i))throw new k(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function IV(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new wp(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new wp(e,n,i)}var ys=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function MV(t,n,e){if(t.isAbsolute)return new ys(n,!0,0);if(!e)return new ys(n,!1,NaN);if(e.parent===null)return new ys(e,!0,0);let i=Dp(t.commands[0])?0:1,r=e.segments.length-1+i;return TV(e,r,t.numberOfDoubleDots)}function TV(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new k(4005,!1);r=i.segments.length}return new ys(i,!1,r-o)}function AV(t){return Ac(t[0])?t[0].outlets:{[ye]:t}}function YI(t,n,e){if(t??=new He([],{}),t.segments.length===0&&t.hasChildren())return Sc(t,n,e);let i=RV(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new He(t.segments.slice(0,i.pathIndex),{});return o.children[ye]=new He(t.segments.slice(i.pathIndex),t.children),Sc(o,0,r)}else return i.match&&r.length===0?new He(t.segments,{}):i.match&&!t.hasChildren()?Dy(t,n,e):i.match?Sc(t,0,r):Dy(t,n,e)}function Sc(t,n,e){if(e.length===0)return new He(t.segments,{});{let i=AV(e),r={};if(Object.keys(i).some(o=>o!==ye)&&t.children[ye]&&t.numberOfChildren===1&&t.children[ye].segments.length===0){let o=Sc(t.children[ye],n,e);return new He(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=YI(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new He(t.segments,r)}}function RV(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(Ac(s))break;let l=`${s}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!AI(l,c,a))return o;i+=2}else{if(!AI(l,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Dy(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Ac(o)){let l=kV(o.outlets);return new He(i,l)}if(r===0&&Dp(e[0])){let l=t.segments[n];i.push(new eo(l.path,TI(e[0]))),r++;continue}let a=Ac(o)?o.outlets[ye]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&Dp(s)?(i.push(new eo(a,TI(s))),r+=2):(i.push(new eo(a,{})),r++)}return new He(i,{})}function kV(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Dy(new He([],{}),0,i))}),n}function TI(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function AI(t,n,e){return t==e.path&&Bi(n,e.parameters)}var Ic="imperative",Ot=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Ot||{}),Cn=class{id;url;constructor(n,e){this.id=n,this.url=e}},ea=class extends Cn{type=Ot.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ci=class extends Cn{urlAfterRedirects;type=Ot.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},$t=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})($t||{}),Rc=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Rc||{}),Pn=class extends Cn{reason;code;type=Ot.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function QI(t){return t instanceof Pn&&(t.code===$t.Redirect||t.code===$t.SupersededByNewNavigation)}var dr=class extends Cn{reason;code;type=Ot.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ta=class extends Cn{error;target;type=Ot.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},kc=class extends Cn{urlAfterRedirects;state;type=Ot.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Ep=class extends Cn{urlAfterRedirects;state;type=Ot.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},xp=class extends Cn{urlAfterRedirects;state;shouldActivate;type=Ot.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Sp=class extends Cn{urlAfterRedirects;state;type=Ot.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Ip=class extends Cn{urlAfterRedirects;state;type=Ot.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Mp=class{route;type=Ot.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Tp=class{route;type=Ot.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Ap=class{snapshot;type=Ot.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Rp=class{snapshot;type=Ot.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},kp=class{snapshot;type=Ot.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Op=class{snapshot;type=Ot.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Cs=class{},Oc=class{},Ds=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function OV(t){return!(t instanceof Cs)&&!(t instanceof Ds)&&!(t instanceof Oc)}var Np=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Is(this.rootInjector)}},Is=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Np(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(ne(ze))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Pp=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=wy(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=wy(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Ey(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Ey(n,this._root).map(e=>e.value)}};function wy(t,n){if(t===n.value)return n;for(let e of n.children){let i=wy(t,e);if(i)return i}return null}function Ey(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Ey(t,e);if(i.length)return i.unshift(n),i}return[]}var bn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function _s(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var Nc=class extends Pp{snapshot;constructor(n,e){super(n),this.snapshot=e,Py(this,n)}toString(){return this.snapshot.toString()}};function ZI(t,n){let e=NV(t,n),i=new mt([new eo("",{})]),r=new mt({}),o=new mt({}),a=new mt({}),s=new mt(""),l=new ur(i,r,a,s,o,ye,t,e.root);return l.snapshot=e.root,new Nc(new bn(l,[]),e)}function NV(t,n){let e={},i={},r={},a=new ws([],e,r,"",i,ye,t,null,{},n);return new Pc("",new bn(a,[]))}var ur=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(ce(c=>c[Vc]))??Q(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ce(n=>Jo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ce(n=>Jo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Ny(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:D(D({},n.params),t.params),data:D(D({},n.data),t.data),resolve:D(D(D(D({},t.data),n.data),r?.data),t._resolvedData)}:i={params:D({},t.params),data:D({},t.data),resolve:D(D({},t.data),t._resolvedData??{})},r&&XI(r)&&(i.resolve[Vc]=r.title),i}var ws=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Vc]}constructor(n,e,i,r,o,a,s,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Jo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Jo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},Pc=class extends Pp{url;constructor(n,e){super(e),this.url=n,Py(this,e)}toString(){return KI(this._root)}};function Py(t,n){n.value._routerState=t,n.children.forEach(e=>Py(t,e))}function KI(t){let n=t.children.length>0?` { ${t.children.map(KI).join(", ")} } `:"";return`${t.value}${n}`}function gy(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Bi(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Bi(n.params,e.params)||t.paramsSubject.next(e.params),lV(n.url,e.url)||t.urlSubject.next(e.url),Bi(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function xy(t,n){let e=Bi(t.params,n.params)&&pV(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||xy(t.parent,n.parent))}function XI(t){return typeof t.title=="string"||t.title===null}var JI=new y(""),ia=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=ye;activateEvents=new $;deactivateEvents=new $;attachEvents=new $;detachEvents=new $;routerOutletData=np();parentContexts=d(Is);location=d(Je);changeDetector=d(Re);inputBinder=d(jc,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new k(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new k(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new k(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new k(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new Sy(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ae]})}return t})(),Sy=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===ur?this.route:n===Is?this.childContexts:n===JI?this.outletData:this.parent.get(n,e)}},jc=new y(""),Fy=(()=>{class t{outletDataSubscriptions=new Map;bindActivatedRouteToOutletComponent(e){this.unsubscribeFromRouteData(e),this.subscribeToRouteData(e)}unsubscribeFromRouteData(e){this.outletDataSubscriptions.get(e)?.unsubscribe(),this.outletDataSubscriptions.delete(e)}subscribeToRouteData(e){let{activatedRoute:i}=e,r=Ci([i.queryParams,i.params,i.data]).pipe(at(([o,a,s],l)=>(s=D(D(D({},o),a),s),l===0?Q(s):Promise.resolve(s)))).subscribe(o=>{if(!e.isActivated||!e.activatedComponentRef||e.activatedRoute!==i||i.component===null){this.unsubscribeFromRouteData(e);return}let a=zS(i.component);if(!a){this.unsubscribeFromRouteData(e);return}for(let{templateName:s}of a.inputs)e.activatedComponentRef.setInput(s,o[s])});this.outletDataSubscriptions.set(e,r)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Ly=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&X(0,"router-outlet")},dependencies:[ia],encapsulation:2})}return t})();function Vy(t){let n=t.children&&t.children.map(Vy),e=n?se(D({},t),{children:n}):D({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==ye&&(e.component=Ly),e}function PV(t,n,e){let i=Fc(t,n._root,e?e._root:void 0);return new Nc(i,n)}function Fc(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=FV(t,n,e);return new bn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let a=o.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(s=>Fc(t,s)),a}}let i=LV(n.value),r=n.children.map(o=>Fc(t,o));return new bn(i,r)}}function FV(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return Fc(t,i,r);return Fc(t,i)})}function LV(t){return new ur(new mt(t.url),new mt(t.params),new mt(t.queryParams),new mt(t.fragment),new mt(t.data),t.outlet,t.component,t)}var Es=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},eM="ngNavigationCancelingError";function Fp(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=no(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=tM(!1,$t.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function tM(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[eM]=!0,e.cancellationCode=n,e}function VV(t){return nM(t)&&no(t.url)}function nM(t){return!!t&&t[eM]}var Iy=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),gy(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=_s(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=_s(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=_s(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=_s(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Op(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Rp(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(gy(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),gy(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},Lp=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},bs=class{component;route;constructor(n,e){this.component=n,this.route=e}};function jV(t,n,e){let i=t._root,r=n?n._root:null;return xc(i,r,e,[i.value])}function BV(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Ms(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Nm(t)?t:n.get(t):i}function xc(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=_s(n);return t.children.forEach(a=>{HV(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>Mc(s,e.getContext(a),r)),r}function HV(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=UV(a,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Lp(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?xc(t,n,s?s.children:null,i,r):xc(t,n,e,i,r),l&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new bs(s.outlet.component,a))}else a&&Mc(n,s,r),r.canActivateChecks.push(new Lp(i)),o.component?xc(t,null,s?s.children:null,i,r):xc(t,null,e,i,r);return r}function UV(t,n,e){if(typeof e=="function")return kt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Xo(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Xo(t.url,n.url)||!Bi(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!xy(t,n)||!Bi(t.queryParams,n.queryParams);default:return!xy(t,n)}}function Mc(t,n,e){let i=_s(t),r=t.value;Object.entries(i).forEach(([o,a])=>{r.component?n?Mc(a,n.children.getContext(o),e):Mc(a,null,e):Mc(a,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new bs(n.outlet.component,r)):e.canDeactivateChecks.push(new bs(null,r)):e.canDeactivateChecks.push(new bs(null,r))}function Bc(t){return typeof t=="function"}function zV(t){return typeof t=="boolean"}function $V(t){return t&&Bc(t.canLoad)}function GV(t){return t&&Bc(t.canActivate)}function WV(t){return t&&Bc(t.canActivateChild)}function qV(t){return t&&Bc(t.canDeactivate)}function YV(t){return t&&Bc(t.canMatch)}function iM(t){return t instanceof Mo||t?.name==="EmptyError"}var vp=Symbol("INITIAL_VALUE");function xs(){return at(t=>Ci(t.map(n=>n.pipe(Xe(1),$e(vp)))).pipe(ce(n=>{for(let e of n)if(e!==!0){if(e===vp)return vp;if(e===!1||QV(e))return e}return!0}),Ee(n=>n!==vp),Xe(1)))}function QV(t){return no(t)||t instanceof Es}function rM(t){return t.aborted?Q(void 0).pipe(Xe(1)):new he(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function oM(t){return xe(rM(t))}function ZV(t){return Bt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Q(se(D({},n),{guardsResult:!0})):KV(o,e,i).pipe(Bt(a=>a&&zV(a)?XV(e,r,t):Q(a)),ce(a=>se(D({},n),{guardsResult:a})))})}function KV(t,n,e){return Ke(t).pipe(Bt(i=>ij(i.component,i.route,e,n)),Xi(i=>i!==!0,!0))}function XV(t,n,e){return Ke(n).pipe(To(i=>Pr(ej(i.route.parent,e),JV(i.route,e),nj(t,i.path),tj(t,i.route))),Xi(i=>i!==!0,!0))}function JV(t,n){return t!==null&&n&&n(new kp(t)),Q(!0)}function ej(t,n){return t!==null&&n&&n(new Ap(t)),Q(!0)}function tj(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Q(!0);let i=e.map(r=>Yn(()=>{let o=n._environmentInjector,a=Ms(r,o),s=GV(a)?a.canActivate(n,t):kt(o,()=>a(n,t));return na(s).pipe(Xi())}));return Q(i).pipe(xs())}function nj(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>BV(o)).filter(o=>o!==null).map(o=>Yn(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=Ms(s,l),u=WV(c)?c.canActivateChild(e,t):kt(l,()=>c(e,t));return na(u).pipe(Xi())});return Q(a).pipe(xs())}));return Q(r).pipe(xs())}function ij(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Q(!0);let o=r.map(a=>{let s=n._environmentInjector,l=Ms(a,s),c=qV(l)?l.canDeactivate(t,n,e,i):kt(s,()=>l(t,n,e,i));return na(c).pipe(Xi())});return Q(o).pipe(xs())}function rj(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Q(!0);let a=o.map(s=>{let l=Ms(s,t),c=$V(l)?l.canLoad(n,e):kt(t,()=>l(n,e)),u=na(c);return r?u.pipe(oM(r)):u});return Q(a).pipe(xs(),aM(i))}function aM(t){return dm(Rt(n=>{if(typeof n!="boolean")throw Fp(t,n)}),ce(n=>n===!0))}function oj(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return Q(!0);let s=a.map(l=>{let c=Ms(l,t),u=YV(c)?c.canMatch(n,e,r):kt(t,()=>c(n,e,r));return na(u).pipe(oM(o))});return Q(s).pipe(xs(),aM(i))}var cr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Lc=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function aj(t){throw new k(4e3,!1)}function sj(t){throw tM(!1,$t.GuardRejected)}var My=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[ye])throw aj(`${n.redirectTo}`);r=r.children[ye]}}async applyRedirectCommands(n,e,i,r,o){let a=await lj(e,r,o);if(a instanceof Jt)throw new Lc(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new Lc(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new Jt(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a={};return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,i,r)}),new He(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new k(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function lj(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Cp(na(kt(e,()=>i(n))))}function cj(t,n){return t.providers&&!t._injector&&(t._injector=rs(t.providers,n,`Route: ${t.path}`)),t._injector??n}function li(t){return t.outlet||ye}function dj(t,n){let e=t.filter(i=>li(i)===n);return e.push(...t.filter(i=>li(i)!==n)),e}var Ty={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function sM(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function uj(t,n,e,i,r,o,a){let s=lM(t,n,e);if(!s.matched)return Q(s);let l=sM(o(s));return i=cj(n,i),oj(i,n,e,r,l,a).pipe(ce(c=>c===!0?s:D({},Ty)))}function lM(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?D({},Ty):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||OI)(e,t,n);if(!r)return D({},Ty);let o={};Object.entries(r.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=r.consumed.length>0?D(D({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function RI(t,n,e,i,r){return e.length>0&&hj(t,e,i,r)?{segmentGroup:new He(n,pj(i,new He(e,t.children))),slicedSegments:[]}:e.length===0&&mj(t,e,i)?{segmentGroup:new He(t.segments,fj(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new He(t.segments,t.children),slicedSegments:e}}function fj(t,n,e,i){let r={};for(let o of e)if(jp(t,n,o)&&!i[li(o)]){let a=new He([],{});r[li(o)]=a}return D(D({},i),r)}function pj(t,n){let e={};e[ye]=n;for(let i of t)if(i.path===""&&li(i)!==ye){let r=new He([],{});e[li(i)]=r}return e}function hj(t,n,e,i){return e.some(r=>!jp(t,n,r)||!(li(r)!==ye)?!1:!(i!==void 0&&li(r)===i))}function mj(t,n,e){return e.some(i=>jp(t,n,i))}function jp(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function gj(t,n,e){return n.length===0&&!t.children[e]}var Ay=class{};async function vj(t,n,e,i,r,o,a="emptyOnly",s){return new Ry(t,n,e,i,r,a,o,s).recognize()}var _j=31,Ry=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new My(this.urlSerializer,this.urlTree)}noMatchError(n){return new k(4002,`'${n.segmentGroup}'`)}async recognize(){let n=RI(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new bn(i,e),o=new Pc("",r),a=$I(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new ws([],Object.freeze({}),Object.freeze(D({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),ye,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,ye,e),rootSnapshot:e}}catch(i){if(i instanceof Lc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof cr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof bn?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=i.children[l],u=dj(e,l),f=await this.processSegmentGroup(n,u,c,l,r);a.push(...f)}let s=cM(a);return yj(s),s}async processSegment(n,e,i,r,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,a,s)}catch(c){if(c instanceof cr||iM(c))continue;throw c}if(gj(i,r,o))return new Ay;throw new cr(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,l){if(li(i)!==a&&(a===ye||!jp(r,o,i)))throw new cr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,l);throw new cr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:p}=lM(e,r,o);if(!l)throw new cr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>_j&&(this.allowRedirects=!1));let h=this.createSnapshot(n,r,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let _=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,sM(h),n),x=await this.applyRedirects.lineralizeSegments(r,_);return this.processSegment(n,i,e,x.concat(p),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new ws(i,r,Object.freeze(D({},this.urlTree.queryParams)),this.urlTree.fragment,Cj(e),li(e),e.component??e._loadedComponent??null,e,Dj(e),n),s=Ny(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=pt=>this.createSnapshot(n,i,pt.consumedSegments,pt.parameters,a),l=await Cp(uj(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new cr(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:p,remainingSegments:h}=l,_=this.createSnapshot(n,i,p,f,a),{segmentGroup:x,slicedSegments:T}=RI(e,p,h,c,o);if(T.length===0&&x.hasChildren()){let pt=await this.processChildren(u,c,x,_);return new bn(_,pt)}if(c.length===0&&T.length===0)return new bn(_,[]);let N=li(i)===o,pe=await this.processSegment(u,c,x,T,N?ye:o,!0,_);return new bn(_,pe instanceof bn?[pe]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Cp(rj(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw sj(e)}return{routes:[],injector:n}}};function yj(t){t.sort((n,e)=>n.value.outlet===ye?-1:e.value.outlet===ye?1:n.value.outlet.localeCompare(e.value.outlet))}function bj(t){let n=t.value.routeConfig;return n&&n.path===""}function cM(t){let n=[],e=new Set;for(let i of t){if(!bj(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=cM(i.children);n.push(new bn(i.value,r))}return n.filter(i=>!e.has(i))}function Cj(t){return t.data||{}}function Dj(t){return t.resolve||{}}function wj(t,n,e,i,r,o,a){return Bt(async s=>{let{state:l,tree:c}=await vj(t,n,e,i,s.extractedUrl,r,o,a);return se(D({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function Ej(t){return Bt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Q(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let l of dM(s))o.add(l);let a=0;return Ke(o).pipe(To(s=>r.has(s)?xj(s,e,t):(s.data=Ny(s,s.parent,t).resolve,Q(void 0))),Rt(()=>a++),cu(1),Bt(s=>a===o.size?Q(n):ot))})}function dM(t){let n=t.children.map(e=>dM(e)).flat();return[t,...n]}function xj(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!XI(i)&&(r[Vc]=i.title),Yn(()=>(t.data=Ny(t,t.parent,e).resolve,Sj(r,t,n).pipe(ce(o=>(t._resolvedData=o,t.data=D(D({},t.data),o),null)))))}function Sj(t,n,e){let i=_y(t);if(i.length===0)return Q({});let r={};return Ke(i).pipe(Bt(o=>Ij(t[o],n,e).pipe(Xi(),Rt(a=>{if(a instanceof Es)throw Fp(new to,a);r[o]=a}))),cu(1),ce(()=>r),Fr(o=>iM(o)?ot:bl(o)))}function Ij(t,n,e){let i=n._environmentInjector,r=Ms(t,i),o=r.resolve?r.resolve(n,e):kt(i,()=>r(n,e));return na(o)}function kI(t){return at(n=>{let e=t(n);return e?Ke(e).pipe(ce(()=>n)):Q(n)})}var jy=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===ye);return i}getResolvedTitleForRoute(e){return e.data[Vc]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(uM),providedIn:"root"})}return t})(),uM=(()=>{class t extends jy{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(ne(wI))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ts=new y("",{factory:()=>({})}),Hc=new y(""),fM=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(T_);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await PI(kt(e,()=>i.loadComponent())),a=await mM(hM(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await pM(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function pM(t,n,e,i){let r=await PI(kt(e,()=>t.loadChildren())),o=await mM(hM(r)),a;o instanceof jf||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,l,c=!1,u;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,u=a,l=s.get(Hc,[],{optional:!0,self:!0}).flat()),{routes:l.map(Vy),injector:s,factory:u}}function Mj(t){return t&&typeof t=="object"&&"default"in t}function hM(t){return Mj(t)?t.default:t}async function mM(t){return t}var Bp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(Tj),providedIn:"root"})}return t})(),Tj=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gM=new y("");var Aj=()=>{},vM=new y(""),_M=(()=>{class t{currentNavigation=I(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=I(null);events=new S;transitionAbortWithErrorSubject=new S;configLoader=d(fM);environmentInjector=d(ze);destroyRef=d(Kt);urlSerializer=d(Ss);rootContexts=d(Is);location=d(Xr);inputBindingEnabled=d(jc,{optional:!0})!==null;titleStrategy=d(jy);options=d(Ts,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(Bp);createViewTransition=d(gM,{optional:!0});navigationErrorHandler=d(vM,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Q(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new Mp(r)),i=r=>this.events.next(new Tp(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Le(()=>{this.transitions?.next(se(D({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new mt(null),this.transitions.pipe(Ee(i=>i!==null),at(i=>{let r=!1,o=new AbortController,a=()=>!r&&this.currentTransition?.id===i.id;return Q(i).pipe(at(s=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",$t.SupersededByNewNavigation),ot;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:s.id,initialUrl:s.rawUrl,extractedUrl:s.extractedUrl,targetBrowserUrl:typeof s.extras.browserUrl=="string"?this.urlSerializer.parse(s.extras.browserUrl):s.extras.browserUrl,trigger:s.source,extras:s.extras,previousNavigation:l?se(D({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:s.routesRecognizeHandler,beforeActivateHandler:s.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=s.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new dr(s.id,this.urlSerializer.serialize(s.rawUrl),"",Rc.IgnoredSameUrlNavigation)),s.resolve(!1),ot;if(this.urlHandlingStrategy.shouldProcessUrl(s.rawUrl))return Q(s).pipe(at(f=>(this.events.next(new ea(f.id,this.urlSerializer.serialize(f.extractedUrl),f.source,f.restoredState)),f.id!==this.navigationId?ot:Promise.resolve(f))),wj(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),Rt(f=>{i.targetSnapshot=f.targetSnapshot,i.urlAfterRedirects=f.urlAfterRedirects,this.currentNavigation.update(p=>(p.finalUrl=f.urlAfterRedirects,p)),this.events.next(new Oc)}),at(f=>Ke(i.routesRecognizeHandler.deferredHandle??Q(void 0)).pipe(ce(()=>f))),Rt(()=>{let f=new kc(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(f)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(s.currentRawUrl)){let{id:f,extractedUrl:p,source:h,restoredState:_,extras:x}=s,T=new ea(f,this.urlSerializer.serialize(p),h,_);this.events.next(T);let N=ZI(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=se(D({},s),{targetSnapshot:N,urlAfterRedirects:p,extras:se(D({},x),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(pe=>(pe.finalUrl=p,pe)),Q(i)}else return this.events.next(new dr(s.id,this.urlSerializer.serialize(s.extractedUrl),"",Rc.IgnoredByUrlHandlingStrategy)),s.resolve(!1),ot}),ce(s=>{let l=new Ep(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);return this.events.next(l),this.currentTransition=i=se(D({},s),{guards:jV(s.targetSnapshot,s.currentSnapshot,this.rootContexts)}),i}),ZV(s=>this.events.next(s)),at(s=>{if(i.guardsResult=s.guardsResult,s.guardsResult&&typeof s.guardsResult!="boolean")throw Fp(this.urlSerializer,s.guardsResult);let l=new xp(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot,!!s.guardsResult);if(this.events.next(l),!a())return ot;if(!s.guardsResult)return this.cancelNavigationTransition(s,"",$t.GuardRejected),ot;if(s.guards.canActivateChecks.length===0)return Q(s);let c=new Sp(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);if(this.events.next(c),!a())return ot;let u=!1;return Q(s).pipe(Ej(this.paramsInheritanceStrategy),Rt({next:()=>{u=!0;let f=new Ip(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects),s.targetSnapshot);this.events.next(f)},complete:()=>{u||this.cancelNavigationTransition(s,"",$t.NoDataFromResolver)}}))}),kI(s=>{let l=u=>{let f=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let p=u._environmentInjector;f.push(this.configLoader.loadComponent(p,u.routeConfig).then(h=>{u.component=h}))}for(let p of u.children)f.push(...l(p));return f},c=l(s.targetSnapshot.root);return c.length===0?Q(s):Ke(Promise.all(c).then(()=>s))}),kI(()=>this.afterPreactivation()),at(()=>{let{currentSnapshot:s,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,s.root,l.root);return c?Ke(c).pipe(ce(()=>i)):Q(i)}),Xe(1),at(s=>{let l=PV(e.routeReuseStrategy,s.targetSnapshot,s.currentRouterState);this.currentTransition=i=s=se(D({},s),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new Cs);let c=i.beforeActivateHandler.deferredHandle;return c?Ke(c.then(()=>s)):Q(s)}),Rt(s=>{new Iy(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),a()&&(r=!0,this.currentNavigation.update(l=>(l.abort=Aj,l)),this.lastSuccessfulNavigation.set(Le(this.currentNavigation)),this.events.next(new ci(s.id,this.urlSerializer.serialize(s.extractedUrl),this.urlSerializer.serialize(s.urlAfterRedirects))),this.titleStrategy?.updateTitle(s.targetRouterState.snapshot),s.resolve(!0))}),xe(rM(o.signal).pipe(Ee(()=>!r&&!i.targetRouterState),Rt(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",$t.Aborted)}))),Rt({complete:()=>{r=!0}}),xe(this.transitionAbortWithErrorSubject.pipe(Rt(s=>{throw s}))),Ao(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",$t.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Fr(s=>{if(r=!0,this.destroyed)return i.resolve(!1),ot;if(nM(s))this.events.next(new Pn(i.id,this.urlSerializer.serialize(i.extractedUrl),s.message,s.cancellationCode)),VV(s)?this.events.next(new Ds(s.url,s.navigationBehaviorOptions)):i.resolve(!1);else{let l=new ta(i.id,this.urlSerializer.serialize(i.extractedUrl),s,i.targetSnapshot??void 0);try{let c=kt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof Es){let{message:u,cancellationCode:f}=Fp(this.urlSerializer,c);this.events.next(new Pn(i.id,this.urlSerializer.serialize(i.extractedUrl),u,f)),this.events.next(new Ds(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),s}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return ot}))}))}cancelNavigationTransition(e,i,r){let o=new Pn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Le(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Rj(t){return t!==Ic}var yM=new y("");var bM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(kj),providedIn:"root"})}return t})(),Vp=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},kj=(()=>{class t extends Vp{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Hp=(()=>{class t{urlSerializer=d(Ss);options=d(Ts,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Xr);urlHandlingStrategy=d(Bp);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Jt;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof Jt?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=ZI(null,d(ze));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(Oj),providedIn:"root"})}return t})(),Oj=(()=>{class t extends Hp{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof ea?this.updateStateMemento():e instanceof dr?this.commitTransition(i):e instanceof kc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Cs?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Pn&&!QI(e)?this.restoreHistory(i):e instanceof ta?this.restoreHistory(i,!0):e instanceof ci&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=D(D({},s),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=D(D({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?D({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):D({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function By(t,n){t.events.pipe(Ee(e=>e instanceof ci||e instanceof Pn||e instanceof ta||e instanceof dr),ce(e=>e instanceof ci||e instanceof dr?0:(e instanceof Pn?e.code===$t.Redirect||e.code===$t.SupersededByNewNavigation:!1)?2:1),Ee(e=>e!==2),Xe(1)).subscribe(()=>{n()})}var Dn=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(Bf);stateManager=d(Hp);options=d(Ts,{optional:!0})||{};pendingTasks=d(Ri);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(_M);urlSerializer=d(Ss);location=d(Xr);urlHandlingStrategy=d(Bp);injector=d(ze);_events=new S;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(bM);injectorCleanup=d(yM,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Hc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(jc,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new le;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Le(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Pn&&i.code!==$t.Redirect&&i.code!==$t.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ci)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Ds){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=D({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||Rj(r.source)},a);this.scheduleNavigation(s,Ic,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}OV(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ic,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=se(D({},o),{browserUrl:e})),r){let c=D({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(s);this.scheduleNavigation(l,i,a,o).catch(c=>{this.disposed||this.injector.get(vn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Le(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Vy),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:a,u=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":u=D(D({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let p=r?r.snapshot:this.routerState.snapshot.root;f=GI(p)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return WI(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=no(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Ic,null,i)}navigate(e,i={skipLocationChange:!1}){return Nj(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(wi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=D({},ky):i===!1?r=D({},Tc):r=D(D({},Tc),i),no(e))return yy(this.currentUrlTree,e,r);let o=this.parseUrl(e);return yy(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,l,c;a?(s=a.resolve,l=a.reject,c=a.promise):c=new Promise((f,p)=>{s=f,l=p});let u=this.pendingTasks.add();return By(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Nj(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new k(4008,!1)}var Fj=(()=>{class t{router=d(Dn);stateManager=d(Hp);fragment=I("");queryParams=I({});path=I("");serializer=d(Ss);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof ci&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new Jt(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),As=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new an("href"),{optional:!0});reactiveHref=qf(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Le(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Le(this._target)}_target=I(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Le(this._queryParams)}_queryParams=I(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Le(this._fragment)}_fragment=I(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Le(this._queryParamsHandling)}_queryParamsHandling=I(void 0);set state(e){this._state.set(e)}get state(){return Le(this._state)}_state=I(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Le(this._info)}_info=I(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Le(this._relativeTo)}_relativeTo=I(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Le(this._preserveFragment)}_preserveFragment=I(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Le(this._skipLocationChange)}_skipLocationChange=I(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Le(this._replaceUrl)}_replaceUrl=I(!1);isAnchorElement;onChanges=new S;applicationErrorHandler=d(vn);options=d(Ts,{optional:!0});reactiveRouterState=d(Fj);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let l=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=I(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(no(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(s,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=xt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:no(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Le(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(ee(Dn),ee(ur),tc("tabindex"),ee(Ge),ee(V),ee(us))};static \u0275dir=E({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&G("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&oe("href",r.reactiveHref(),Uv)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",B],skipLocationChange:[2,"skipLocationChange","skipLocationChange",B],replaceUrl:[2,"replaceUrl","replaceUrl",B],routerLink:"routerLink"},features:[Ae]})}return t})(),Hy=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new $;link=d(As,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(a=>{a instanceof ci&&this.update()})}ngAfterContentInit(){Q(this.links.changes,Q(null)).pipe(Nr()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=Ke(e).pipe(Nr()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=Lj(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?D({},ky):D({},Tc);return r=>{let o=r.urlTree;return o?Le(Oy(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(ee(Dn),ee(V),ee(Ge),ee(Re))};static \u0275dir=E({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&Ze(o,As,5),i&2){let a;U(a=z())&&(r.links=a)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[Ae]})}return t})();function Lj(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var Vj=new y("");function Uy(t,...n){return xi([{provide:Hc,multi:!0,useValue:t},[],{provide:ur,useFactory:jj},{provide:Yr,multi:!0,useFactory:Hj},n.map(e=>e.\u0275providers)])}function jj(){return d(Dn).routerState.root}function Bj(t,n){return{\u0275kind:t,\u0275providers:n}}function Hj(){let t=d(J);return n=>{let e=t.get(Et);if(n!==e.components[0])return;let i=t.get(Dn),r=t.get(Uj);t.get(zj)===1&&i.initialNavigation(),t.get($j,null,{optional:!0})?.setUpPreloading(),t.get(Vj,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var Uj=new y("",{factory:()=>new S}),zj=new y("",{factory:()=>1});var $j=new y("");function zy(){return Bj(8,[Fy,{provide:jc,useExisting:Fy}])}var $y;try{$y=typeof Intl<"u"&&Intl.v8BreakIterator}catch{$y=!1}var Ie=(()=>{class t{_platformId=d(ir);isBrowser=this._platformId?sp(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||$y)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gy;function DM(){if(Gy==null){let t=typeof document<"u"?document.head:null;Gy=!!(t&&(t.createShadowRoot||t.attachShadow))}return Gy}function Wy(t){if(DM()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function ra(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Lt(t){return t.composedPath?t.composedPath()[0]:t.target}function qy(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Up=new WeakMap,ft=(()=>{class t{_appRef;_injector=d(J);_environmentInjector=d(ze);load(e){let i=this._appRef=this._appRef||this._injector.get(Et),r=Up.get(i);r||(r={loaders:new Set,refs:[]},Up.set(i,r),i.onDestroy(()=>{Up.get(i)?.refs.forEach(o=>o.destroy()),Up.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(ip(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function bt(t){return t==null?"":typeof t=="string"?t:`${t}px`}function Rs(t){return Array.isArray(t)?t:[t]}function Hi(t,n=0){return wM(t)?Number(t):arguments.length===2?n:0}function wM(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function wn(t){return t instanceof V?t.nativeElement:t}var Gj=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(j)}),Wj=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function EM(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?Wj.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Ct=(()=>{class t{get value(){return this.valueSignal()}valueSignal=I("ltr");change=new $;constructor(){let e=d(Gj,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(EM(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var di=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(di||{}),zp,oa;function $p(){if(oa==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return oa=!1,oa;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)oa=!0;else{let t=Element.prototype.scrollTo;t?oa=!/\{\s*\[native code\]\s*\}/.test(t.toString()):oa=!1}}return oa}function ks(){if(typeof document!="object"||!document)return di.NORMAL;if(zp==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),zp=di.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,zp=t.scrollLeft===0?di.NEGATED:di.INVERTED),t.remove()}return zp}function Gp(t){return t&&typeof t.connect=="function"&&!(t instanceof ml)}var ui=(function(t){return t[t.REPLACED=0]="REPLACED",t[t.INSERTED=1]="INSERTED",t[t.MOVED=2]="MOVED",t[t.REMOVED=3]="REMOVED",t})(ui||{}),Wp=class{viewCacheSize=20;_viewCache=[];applyChanges(n,e,i,r,o){n.forEachOperation((a,s,l)=>{let c,u;if(a.previousIndex==null){let f=()=>i(a,s,l);c=this._insertView(f,l,e,r(a)),u=c?ui.INSERTED:ui.REPLACED}else l==null?(this._detachAndCacheView(s,e),u=ui.REMOVED):(c=this._moveView(s,l,e,r(a)),u=ui.MOVED);o&&o({context:c?.context,operation:u,record:a})})}detach(){for(let n of this._viewCache)n.destroy();this._viewCache=[]}_insertView(n,e,i,r){let o=this._insertViewFromCache(e,i);if(o){o.context.$implicit=r;return}let a=n();return i.createEmbeddedView(a.templateRef,a.context,a.index)}_detachAndCacheView(n,e){let i=e.detach(n);this._maybeCacheView(i,e)}_moveView(n,e,i,r){let o=i.get(n);return i.move(o,e),o.context.$implicit=r,o}_maybeCacheView(n,e){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(n);else{let i=e.indexOf(n);i===-1?n.destroy():e.remove(i)}}_insertViewFromCache(n,e){let i=this._viewCache.pop();return i&&e.insert(i,n),i||null}};var ge=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})();var qj=20,Uc=(()=>{class t{_ngZone=d(K);_platform=d(Ie);_renderer=d(wt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new S;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=qj){return this._platform.isBrowser?new he(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Ra(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Q()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ee(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=wn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),xM=(()=>{class t{elementRef=d(V);scrollDispatcher=d(Uc);ngZone=d(K);dir=d(Ct,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new S;_renderer=d(Ge);_cleanupScroll;_elementScrolled=new S;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&ks()!=di.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),ks()==di.INVERTED?e.left=e.right:ks()==di.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;$p()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&ks()==di.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&ks()==di.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),Yj=20,Ui=(()=>{class t{_platform=d(Ie);_listeners;_viewportSize=null;_change=new S;_document=d(j);constructor(){let e=d(K),i=d(wt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=Yj){return e>0?this._change.pipe(Ra(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var SM=new y("CDK_VIRTUAL_SCROLL_VIEWPORT");var Os=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})(),zc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge,Os,ge,Os]})}return t})();var Yy={},Ve=class t{_appId=d(kn);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),Yy.hasOwnProperty(n)||(Yy[n]=0),`${n}${e?t._infix+"-":""}${Yy[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var $c=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},fr=class extends $c{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},en=class extends $c{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Qy=class extends $c{element;constructor(n){super(),this.element=n instanceof V?n.nativeElement:n}},io=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof fr)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof en)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Qy)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Gc=class extends io{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Ni,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||J.NULL,o=r.get(ze,i.injector);e=ip(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Fn=(()=>{class t extends io{_moduleRef=d(Ni,{optional:!0});_document=d(j);_viewContainerRef=d(Je);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new $;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[te]})}return t})(),Ln=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})();function St(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var IM=$p();function Fs(t){return new qp(t.get(Ui),t.get(j))}var qp=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=bt(-this._previousScrollPosition.left),n.style.top=bt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),IM&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),IM&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function NM(t,n){return new Yp(t.get(Uc),t.get(K),t.get(Ui),n)}var Yp=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Ee(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Wc=class{enable(){}disable(){}attach(){}};function Zy(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function MM(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function la(t,n){return new Qp(t.get(Uc),t.get(Ui),t.get(K),n)}var Qp=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Zy(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},PM=(()=>{class t{_injector=d(J);constructor(){}noop=()=>new Wc;close=e=>NM(this._injector,e);block=()=>Fs(this._injector);reposition=e=>la(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),fi=class{positionStrategy;scrollStrategy=new Wc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Zp=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var FM=(()=>{class t{_attachedOverlays=[];_document=d(j);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),LM=(()=>{class t extends FM{_ngZone=d(K);_renderer=d(wt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),VM=(()=>{class t extends FM{_platform=d(Ie);_ngZone=d(K);_renderer=d(wt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Lt(e)};_clickListener=e=>{let i=Lt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(TM(s.overlayElement,i)||TM(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function TM(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var jM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2,changeDetection:0})}return t})(),Jp=(()=>{class t{_platform=d(Ie);_containerElement;_document=d(j);_styleLoader=d(ft);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||qy()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),qy()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(jM)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ky=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Xy(t){return t&&t.nodeType===1}var Ns=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new S;_attachments=new S;_detachments=new S;_positionStrategy;_scrollStrategy;_locationChanges=le.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new S;_outsidePointerEvents=new S;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,u=!1,f,p){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=p,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=lt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=D(D({},this._config),n),this._updateElementSize()}setDirection(n){this._config=se(D({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=bt(this._config.width),n.height=bt(this._config.height),n.minWidth=bt(this._config.minWidth),n.minHeight=bt(this._config.minHeight),n.maxWidth=bt(this._config.maxWidth),n.maxHeight=bt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Xy(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Ky(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Rs(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=lt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},AM="cdk-overlay-connected-position-bounding-box",Zj=/([A-Za-z%]+)$/;function qc(t,n){return new Kp(n,t.get(Ui),t.get(j),t.get(Ie),t.get(Jp))}var Kp=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new S;_resizeSubscription=le.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(AM),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,r,s),c=this._getOverlayPoint(l,e,s),u=this._getOverlayFit(c,e,i,s);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<u.visibleArea)&&(a={overlayFit:u,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&aa(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(AM),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof V?this._origin.nativeElement:Xy(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=kM(e),{x:a,y:s}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(a+=l),c&&(s+=c);let u=0-a,f=a+o.width-i.width,p=0-s,h=s+o.height-i.height,_=this._subtractOverflows(o.width,u,f),x=this._subtractOverflows(o.height,p,h),T=_*x;return{visibleArea:T,isCompletelyWithinViewport:o.width*o.height===T,fitsInViewportVertically:x===o.height,fitsInViewportHorizontally:_==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=RM(this._overlayRef.getConfig().minHeight),s=RM(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=r,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=kM(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-a:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-s:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!Kj(this._lastScrollVisibility,i)){let r=new Zp(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let h=Math.min(i.bottom-n.y+i.top,n.y),_=this._lastBoundingBoxSize.height;o=h*2,a=n.y-h,o>_&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-_/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,p;if(c)p=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let h=Math.min(i.right-n.x+i.left,n.x),_=this._lastBoundingBoxSize.width;u=h*2,f=n.x-h,u>_&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-_/2)}return{top:a,left:f,bottom:s,right:p,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=bt(i.width),r.height=bt(i.height),r.top=bt(i.top)||"auto",r.bottom=bt(i.bottom)||"auto",r.left=bt(i.left)||"auto",r.right=bt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=bt(o)),a&&(r.maxWidth=bt(a))}this._lastBoundingBoxSize=i,aa(this._boundingBox.style,r)}_resetBoundingBoxStyles(){aa(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){aa(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();aa(i,this._getExactOverlayY(e,n,u)),aa(i,this._getExactOverlayX(e,n,u))}else i.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=bt(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=bt(a.maxWidth):o&&(i.maxWidth="")),aa(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=bt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=bt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:MM(n,i),isOriginOutsideView:Zy(n,i),isOverlayClipped:MM(e,i),isOverlayOutsideView:Zy(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Rs(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof V)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function aa(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function RM(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(Zj);return!e||e==="px"?parseFloat(n):null}return t||null}function kM(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function Kj(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var OM="cdk-global-overlay-wrapper";function ro(t){return new Xp}var Xp=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(OM),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),u=this._xPosition,f=this._xOffset,p=this._overlayRef.getConfig().direction==="rtl",h="",_="",x="";l?x="flex-start":u==="center"?(x="center",p?_=f:h=f):p?u==="left"||u==="end"?(x="flex-end",h=f):(u==="right"||u==="start")&&(x="flex-start",_=f):u==="left"||u==="start"?(x="flex-start",h=f):(u==="right"||u==="end")&&(x="flex-end",_=f),n.position=this._cssPosition,n.marginLeft=l?"0":h,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":_,e.justifyContent=x,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(OM),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},BM=(()=>{class t{_injector=d(J);constructor(){}global(){return ro()}flexibleConnectedTo(e){return qc(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Yc=new y("OVERLAY_DEFAULT_CONFIG");function pr(t,n){t.get(ft).load(jM);let e=t.get(Jp),i=t.get(j),r=t.get(Ve),o=t.get(Et),a=t.get(Ct),s=t.get(Ge,null,{optional:!0})||t.get(wt).createRenderer(null,null),l=new fi(n),c=t.get(Yc,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let p=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Xy(p)?p.after(f):p?.type==="parent"?p.element.appendChild(f):e.getContainerElement().appendChild(f),new Ns(new Gc(u,o,t),f,u,l,t.get(K),t.get(LM),i,t.get(Xr),t.get(VM),n?.disableAnimations??t.get(nc,null,{optional:!0})==="NoopAnimations",t.get(ze),s)}var HM=(()=>{class t{scrollStrategies=d(PM);_positionBuilder=d(BM);_injector=d(J);constructor(){}create(e){return pr(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Xj=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],Jj=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(J);return()=>la(t)}}),Ps=(()=>{class t{elementRef=d(V);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),UM=new y("cdk-connected-overlay-default-config"),eh=(()=>{class t{_dir=d(Ct,{optional:!0});_injector=d(J);_overlayRef;_templatePortal;_backdropSubscription=le.EMPTY;_attachSubscription=le.EMPTY;_detachSubscription=le.EMPTY;_positionSubscription=le.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(Jj);_ngZone=d(K);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new $;positionChange=new $;attach=new $;detach=new $;overlayKeydown=new $;overlayOutsideClick=new $;constructor(){let e=d(Ne),i=d(Je),r=d(UM,{optional:!0}),o=d(Yc,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new en(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=Xj);let e=this._overlayRef=pr(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!St(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Lt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new fi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=qc(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ps?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ps?this.origin.elementRef.nativeElement:this.origin instanceof V?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(_m(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",B],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",B],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",B],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",B],push:[2,"cdkConnectedOverlayPush","push",B],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",B],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",B],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Ae]})}return t})(),zi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({providers:[HM],imports:[ge,Ln,zc,zc]})}return t})();function ca(t){return t.buttons===0||t.detail===0}function da(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Qc;function zM(){if(Qc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Qc=!0}))}finally{Qc=Qc||!1}return Qc}function Ls(t){return zM()?t:!!t.capture}var $M=new y("cdk-input-modality-detector-options"),GM={ignoreKeys:[18,17,224,91,16]},WM=650,Jy={passive:!0,capture:!0},qM=(()=>{class t{_platform=d(Ie);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new mt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Lt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<WM||(this._modality.next(ca(e)?"keyboard":"mouse"),this._mostRecentTarget=Lt(e))};_onTouchstart=e=>{if(da(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Lt(e)};constructor(){let e=d(K),i=d(j),r=d($M,{optional:!0});if(this._options=D(D({},GM),r),this.modalityDetected=this._modality.pipe(El(1)),this.modalityChanged=this.modalityDetected.pipe(lu()),this._platform.isBrowser){let o=d(wt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Jy),o.listen(i,"mousedown",this._onMousedown,Jy),o.listen(i,"touchstart",this._onTouchstart,Jy)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Zc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Zc||{}),YM=new y("cdk-focus-monitor-default-options"),th=Ls({passive:!0,capture:!0}),sn=(()=>{class t{_ngZone=d(K);_platform=d(Ie);_inputModalityDetector=d(qM);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(j);_stopInputModalityDetector=new S;constructor(){let e=d(YM,{optional:!0});this._detectionMode=e?.detectionMode||Zc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Lt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=wn(e);if(!this._platform.isBrowser||r.nodeType!==1)return Q();let o=Wy(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new S,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=wn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=wn(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Zc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Zc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?WM:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Lt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,th),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,th)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(xe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,th),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,th),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Vs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2,changeDetection:0})}return t})(),nh;function eB(){if(nh===void 0&&(nh=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(nh=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return nh}function ua(t){return eB()?.createHTML(t)||t}function QM(t,n,e){let i=e.sanitize(Tt.HTML,n);t.innerHTML=ua(i||"")}var ZM=new Set,fa,ih=(()=>{class t{_platform=d(Ie);_nonce=d(Yo,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):nB}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&tB(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function tB(t,n){if(!ZM.has(t))try{fa||(fa=document.createElement("style"),n&&fa.setAttribute("nonce",n),fa.setAttribute("type","text/css"),document.head.appendChild(fa)),fa.sheet&&(fa.sheet.insertRule(`@media ${t} {body{ }}`,0),ZM.add(t))}catch(e){console.error(e)}}function nB(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Kc=(()=>{class t{_mediaMatcher=d(ih);_zone=d(K);_queries=new Map;_destroySubject=new S;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return KM(Rs(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=KM(Rs(e)).map(a=>this._registerQuery(a).observable),o=Ci(r);return o=Pr(o.pipe(Xe(1)),o.pipe(El(1),Dl(0))),o.pipe(ce(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new he(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe($e(i),ce(({matches:a})=>({query:e,matches:a})),xe(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function KM(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var iB=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var rh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({providers:[iB]})}return t})();var nb=(()=>{class t{_platform=d(Ie);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return oB(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=rB(pB(e));if(i&&(XM(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=XM(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!uB(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return fB(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function rB(t){try{return t.frameElement}catch{return null}}function oB(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function aB(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function sB(t){return cB(t)&&t.type=="hidden"}function lB(t){return dB(t)&&t.hasAttribute("href")}function cB(t){return t.nodeName.toLowerCase()=="input"}function dB(t){return t.nodeName.toLowerCase()=="a"}function tT(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function XM(t){if(!tT(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function uB(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function fB(t){return sB(t)?!1:aB(t)||lB(t)||t.hasAttribute("contenteditable")||tT(t)}function pB(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var tb=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?lt(n,{injector:this._injector}):setTimeout(n)}},ib=(()=>{class t{_checker=d(nb);_ngZone=d(K);_document=d(j);_injector=d(J);constructor(){d(ft).load(Vs)}create(e,i=!1){return new tb(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var nT=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),iT=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),hB=0,Xc=(()=>{class t{_ngZone=d(K);_defaultOptions=d(iT,{optional:!0});_liveElement;_document=d(j);_sanitizer=d(wc);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(nT,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:QM(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${hB++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var oo=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(oo||{}),JM="cdk-high-contrast-black-on-white",eT="cdk-high-contrast-white-on-black",eb="cdk-high-contrast-active",rT=(()=>{class t{_platform=d(Ie);_hasCheckedHighContrastMode=!1;_document=d(j);_breakpointSubscription;constructor(){this._breakpointSubscription=d(Kc).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return oo.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return oo.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return oo.BLACK_ON_WHITE}return oo.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(eb,JM,eT),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===oo.BLACK_ON_WHITE?e.add(eb,JM):i===oo.WHITE_ON_BLACK&&e.add(eb,eT)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),rb=(()=>{class t{constructor(){d(rT)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[rh]})}return t})();function mB(t,n){}var ao=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var ab=(()=>{class t extends io{_elementRef=d(V);_focusTrapFactory=d(ib);_config;_interactivityChecker=d(nb);_ngZone=d(K);_focusMonitor=d(sn);_renderer=d(Ge);_changeDetectorRef=d(Re);_injector=d(J);_platform=d(Ie);_document=d(j);_portalOutlet;_focusTrapped=new S;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(ao,{optional:!0})||new ao,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||lt(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=ra(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=ra();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=ra()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&et(Fn,7),i&2){let o;U(o=z())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&oe("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[te],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&ue(0,mB,0,0,"ng-template",0)},dependencies:[Fn],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),Jc=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new S;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!St(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},gB=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(J);return()=>Fs(t)}}),vB=new y("DialogData"),_B=new y("DefaultDialogConfig");function yB(t){let n=I(t),e=new $;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var sb=(()=>{class t{_injector=d(J);_defaultOptions=d(_B,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Jp);_idGenerator=d(Ve);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new S;_afterOpenedAtThisLevel=new S;_ariaHiddenElements=new Map;_scrollStrategy=d(gB);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=Yn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe($e(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new ao;i=D(D({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=pr(this._injector,o),s=new Jc(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Xe(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){ob(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){ob(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),ob(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new fi({positionStrategy:e.positionStrategy||ro().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:ao,useValue:r},{provide:Jc,useValue:i},{provide:Ns,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=ab;let l=new fr(s,r.viewContainerRef,J.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Ne){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=D(D({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new en(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new fr(e,o.viewContainerRef,a));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:vB,useValue:e.data},{provide:Jc,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(Ct,null,{optional:!0}))&&s.push({provide:Ct,useValue:yB(e.direction)}),J.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ob(t,n){let e=t.length;for(;e--;)n(t[e])}var oT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({providers:[sb],imports:[zi,Ln,rb,Ln]})}return t})();function hr(t){return t!=null&&`${t}`!="false"}var aT={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var bB=new y("MATERIAL_ANIMATIONS"),sT=null;function CB(){return d(bB,{optional:!0})?.animationsDisabled||d(nc,{optional:!0})==="NoopAnimations"?"di-disabled":(sT??=d(ih).matchMedia("(prefers-reduced-motion)").matches,sT?"reduced-motion":"enabled")}function qe(){return CB()!=="enabled"}var DB=200,oh=class{_letterKeyStream=new S;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new S;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:DB;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Rt(e=>this._pressedLetters.push(e)),Dl(n),Ee(()=>this._pressedLetters.length>0),ce(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var js=class{_items;_activeItemIndex=I(-1);_activeItem=I(null);_wrap=!1;_typeaheadSubscription=le.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Ut?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Pi(n)&&(this._effectRef=ti(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new S;change=new S;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new oh(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||St(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Pi(this._items)?this._items():this._items instanceof Ut?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var rd=class extends js{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var mr=class extends js{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var cT=" ";function dT(t,n,e){let i=uT(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(cT)))}function lb(t,n,e){let i=uT(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(cT)):t.removeAttribute(n)}function uT(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}function wB(t,n){}var Bs=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},cb="mdc-dialog--open",fT="mdc-dialog--opening",pT="mdc-dialog--closing",EB=150,xB=75,SB=(()=>{class t extends ab{_animationStateChanged=new $;_animationsEnabled=!qe();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?mT(this._config.enterAnimationDuration)??EB:0;_exitAnimationDuration=this._animationsEnabled?mT(this._config.exitAnimationDuration)??xB:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(hT,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(fT,cb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(cb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(cb),this._animationsEnabled?(this._hostElement.style.setProperty(hT,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(pT)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(fT,pT)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(yt("id",r._config.id),oe("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),Y("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[te],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",0)(1,"div",1),ue(2,wB,0,0,"ng-template",2),v()())},dependencies:[Fn],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2})}return t})(),hT="--mat-dialog-transition-duration";function mT(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Hi(t.substring(0,t.length-2)):t.endsWith("s")?Hi(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var ah=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(ah||{}),pi=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Rr(1);_beforeClosed=new Rr(1);_result;_closeFallbackTimeout;_state=ah.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(Ee(r=>r.state==="opened"),Xe(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ee(r=>r.state==="closed"),Xe(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Gt(this.backdropClick(),this.keydownEvents().pipe(Ee(r=>r.keyCode===27&&!this.disableClose&&!St(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),IB(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(Ee(i=>i.state==="closing"),Xe(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=ah.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=ah.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function IB(t,n,e){return t._closeInteractionType=n,t.close(e)}var gr=new y("MatMdcDialogData"),db=new y("mat-mdc-dialog-default-options"),MB=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(J);return()=>Fs(t)}}),pa=(()=>{class t{_defaultOptions=d(db,{optional:!0});_scrollStrategy=d(MB);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ve);_injector=d(J);_dialog=d(sb);_animationsDisabled=qe();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new S;_afterOpenedAtThisLevel=new S;dialogConfigClass=Bs;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=Yn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe($e(void 0)));constructor(){this._dialogRefConstructor=pi,this._dialogContainerType=SB,this._dialogDataToken=gr}open(e,i){let r;i=D(D({},this._defaultOptions||new Bs),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,se(D({},i),{positionStrategy:ro(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:ao,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var so=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[v_([xM])]})}return t})();var gT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({providers:[pa],imports:[oT,zi,Ln,ge]})}return t})();var vT=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=hr(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=hr(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(oe("aria-orientation",r.vertical?"vertical":"horizontal"),Y("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
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
`],encapsulation:2,changeDetection:0})}return t})(),ha=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var Hs,_T=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function fb(){if(Hs)return Hs;if(typeof document!="object"||!document)return Hs=new Set(_T),Hs;let t=document.createElement("input");return Hs=new Set(_T.filter(n=>(t.setAttribute("type",n),t.type===n))),Hs}var Vn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Vn||{}),pb=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Vn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},yT=Ls({passive:!0,capture:!0}),hb=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,yT)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,yT)))}_delegateEventHandler=n=>{let e=Lt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},od={enterDuration:225,exitDuration:150},AB=800,bT=Ls({passive:!0,capture:!0}),CT=["mousedown","touchstart"],DT=["mouseup","mouseleave","touchend","touchcancel"],RB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2,changeDetection:0})}return t})(),ad=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new hb;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=wn(i)),o&&o.get(ft).load(RB)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=D(D({},od),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||kB(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${s-a}px`,u.style.top=`${l-a}px`,u.style.height=`${a*2}px`,u.style.width=`${a*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),p=f.transitionProperty,h=f.transitionDuration,_=p==="none"||h==="0s"||h==="0s, 0s"||r.width===0&&r.height===0,x=new pb(this,u,i,_);u.style.transform="scale3d(1, 1, 1)",x.state=Vn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=x);let T=null;return!_&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let N=()=>{T&&(T.fallbackTimer=null),clearTimeout(pt),this._finishRippleTransition(x)},pe=()=>this._destroyRipple(x),pt=setTimeout(pe,c+100);u.addEventListener("transitionend",N),u.addEventListener("transitioncancel",pe),T={onTransitionEnd:N,onTransitionCancel:pe,fallbackTimer:pt}}),this._activeRipples.set(x,T),(_||!c)&&this._finishRippleTransition(x),x}fadeOutRipple(n){if(n.state===Vn.FADING_OUT||n.state===Vn.HIDDEN)return;let e=n.element,i=D(D({},od),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Vn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=wn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,CT.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{DT.forEach(e=>{this._triggerElement.addEventListener(e,this,bT)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Vn.FADING_IN?this._startFadeOutTransition(n):n.state===Vn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Vn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Vn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ca(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+AB;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!da(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Vn.VISIBLE||n.config.terminateOnPointerUp&&n.state===Vn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(CT.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(DT.forEach(e=>n.removeEventListener(e,this,bT)),this._pointerUpEventsRegistered=!1))}};function kB(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var mb=new y("mat-ripple-global-options"),lo=(()=>{class t{_elementRef=d(V);_animationsDisabled=qe();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(K),i=d(Ie),r=d(mb,{optional:!0}),o=d(J);this._globalOptions=r||{},this._rippleRenderer=new ad(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:D(D(D({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,D(D({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,D(D({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&Y("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var OB={capture:!0},NB=["focus","mousedown","mouseenter","touchstart"],gb="mat-ripple-loader-uninitialized",vb="mat-ripple-loader-class-name",wT="mat-ripple-loader-centered",sh="mat-ripple-loader-disabled",ET=(()=>{class t{_document=d(j);_animationsDisabled=qe();_globalRippleOptions=d(mb,{optional:!0});_platform=d(Ie);_ngZone=d(K);_injector=d(J);_eventCleanups;_hosts=new Map;constructor(){let e=d(wt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>NB.map(i=>e.listen(this._document,i,this._onInteraction,OB)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(gb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(vb))&&e.setAttribute(vb,i.className||""),i.centered&&e.setAttribute(wT,""),i.disabled&&e.setAttribute(sh,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(sh,""):e.removeAttribute(sh)}_onInteraction=e=>{let i=Lt(e);if(i instanceof HTMLElement){let r=i.closest(`[${gb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(vb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??od.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??od.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(sh),rippleConfig:{centered:e.hasAttribute(wT),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new ad(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(gb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var jn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2,changeDetection:0})}return t})();var PB=["mat-icon-button",""],FB=["*"],LB=new y("MAT_BUTTON_CONFIG");function xT(t){return t==null?void 0:si(t)}var lh=(()=>{class t{_elementRef=d(V);_ngZone=d(K);_animationsDisabled=qe();_config=d(LB,{optional:!0});_focusMonitor=d(sn);_cleanupClick;_renderer=d(Ge);_rippleLoader=d(ET);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(ft).load(jn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(oe("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Ft(r.color?"mat-"+r.color:""),Y("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",B],disabled:[2,"disabled","disabled",B],ariaDisabled:[2,"aria-disabled","ariaDisabled",B],disabledInteractive:[2,"disabledInteractive","disabledInteractive",B],tabIndex:[2,"tabIndex","tabIndex",xT],_tabindex:[2,"tabindex","_tabindex",xT]}})}return t})(),ma=(()=>{class t extends lh{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[te],attrs:PB,ngContentSelectors:FB,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(),zt(0,"span",0),W(1),zt(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2,changeDetection:0})}return t})();var co=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var VB=["matButton",""],IT=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],MT=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var jB=["mat-mini-fab",""],BB=`.mat-mdc-fab-base {
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
`,ST=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),tn=(()=>{class t extends lh{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=HB(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?ST.get(this._appearance):null,o=ST.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[te],attrs:VB,ngContentSelectors:MT,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(IT),zt(0,"span",0),W(1),_t(2,"span",1),W(3,1),At(),W(4,2),zt(5,"span",2)(6,"span",3)),i&2&&Y("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2,changeDetection:0})}return t})();function HB(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var UB=new y("mat-mdc-fab-default-options",{providedIn:"root",factory:()=>_b}),_b={color:"accent"};var TT=(()=>{class t extends lh{_options=d(UB,{optional:!0});_isFab=!0;constructor(){super(),this._options=this._options||_b,this.color=this._options.color||_b.color}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["button","mat-mini-fab",""],["a","mat-mini-fab",""],["button","matMiniFab",""],["a","matMiniFab",""]],hostAttrs:[1,"mdc-fab","mat-mdc-fab-base","mdc-fab--mini","mat-mdc-mini-fab"],exportAs:["matButton","matAnchor"],features:[te],attrs:jB,ngContentSelectors:MT,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(IT),zt(0,"span",0),W(1),_t(2,"span",1),W(3,1),At(),W(4,2),zt(5,"span",2)(6,"span",3)),i&2&&Y("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[BB],encapsulation:2,changeDetection:0})}return t})();var ga=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[co,ge]})}return t})();function zB(t,n){if(t&1){let e=rt();m(0,"div",1)(1,"button",2),G("click",function(){je(e);let r=C();return Be(r.action())}),b(2),v()()}if(t&2){let e=C();g(2),Se(" ",e.data.action," ")}}var $B=["label"];function GB(t,n){}var WB=Math.pow(2,31)-1,ld=class{_overlayRef;instance;containerInstance;_afterDismissed=new S;_afterOpened=new S;_onAction=new S;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,WB))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},AT=new y("MatSnackBarData"),uo=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},qB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),YB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),QB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),ZB=(()=>{class t{snackBarRef=d(ld);data=d(AT);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(m(0,"div",0),b(1),v(),A(2,zB,3,1,"div",1)),i&2&&(g(),Se(" ",r.data.message,`
`),g(),R(r.hasAction?2:-1))},dependencies:[tn,qB,YB,QB],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),yb="_mat-snack-bar-enter",bb="_mat-snack-bar-exit",KB=(()=>{class t extends io{_ngZone=d(K);_elementRef=d(V);_changeDetectorRef=d(Re);_platform=d(Ie);_animationsDisabled=qe();snackBarConfig=d(uo);_document=d(j);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(J);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new S;_onExit=new S;_onEnter=new S;_animationState="void";_live;_label;_role;_liveElementId=d(Ve).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===bb?this._completeExit():e===yb&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?lt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(yb)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(yb)},200)))}exit(){return this._destroyed?Q(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?lt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(bb)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(bb),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&et(Fn,7)($B,7),i&2){let o;U(o=z())&&(r._portalOutlet=o.first),U(o=z())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&G("animationend",function(a){return r.onAnimationEnd(a.animationName)})("animationcancel",function(a){return r.onAnimationEnd(a.animationName)}),i&2&&Y("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[te],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",1)(1,"div",2,0)(3,"div",3),ue(4,GB,0,0,"ng-template",4),v(),X(5,"div"),v()()),i&2&&(g(5),oe("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[Fn],styles:[`@keyframes _mat-snack-bar-enter {
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
`],encapsulation:2})}return t})(),XB=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new uo}),En=(()=>{class t{_live=d(Xc);_injector=d(J);_breakpointObserver=d(Kc);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(XB);_animationsDisabled=qe();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=ZB;snackBarContainerComponent=KB;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=D(D({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=J.create({parent:r||this._injector,providers:[{provide:uo,useValue:i}]}),a=new fr(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=D(D(D({},new uo),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new ld(a,o);if(e instanceof Ne){let l=new en(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(l)}else{let l=this._createInjector(r,s),c=new fr(e,void 0,l),u=a.attachComponentPortal(c);s.instance=u.instance}return this._breakpointObserver.observe(aT.HandsetPortrait).pipe(xe(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new fi;i.direction=e.direction;let r=ro(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,pr(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return J.create({parent:r||this._injector,providers:[{provide:ld,useValue:i},{provide:AT,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var jT=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ee(Ge),ee(V))};static \u0275dir=E({type:t})}return t})(),JB=(()=>{class t extends jT{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,features:[te]})}return t})(),gd=new y("");var eH={provide:gd,useExisting:on(()=>Bn),multi:!0};function tH(){let t=Nn()?Nn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var nH=new y(""),Bn=(()=>{class t extends jT{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!tH())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ee(Ge),ee(V),ee(nH,8))};static \u0275dir=E({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&G("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[_e([eH]),te]})}return t})();function Eb(t){return t==null||xb(t)===0}function xb(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var va=new y(""),yh=new y(""),iH=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,q=class{static min(n){return rH(n)}static max(n){return oH(n)}static required(n){return BT(n)}static requiredTrue(n){return aH(n)}static email(n){return sH(n)}static minLength(n){return lH(n)}static maxLength(n){return cH(n)}static pattern(n){return dH(n)}static nullValidator(n){return dh()}static compose(n){return WT(n)}static composeAsync(n){return qT(n)}};function rH(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function oH(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function BT(t){return Eb(t.value)?{required:!0}:null}function aH(t){return t.value===!0?null:{required:!0}}function sH(t){return Eb(t.value)||iH.test(t.value)?null:{email:!0}}function lH(t){return n=>{let e=n.value?.length??xb(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function cH(t){return n=>{let e=n.value?.length??xb(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function dH(t){if(!t)return dh;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Eb(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function dh(t){return null}function HT(t){return t!=null}function UT(t){return qr(t)?Ke(t):t}function zT(t){let n={};return t.forEach(e=>{n=e!=null?D(D({},n),e):n}),Object.keys(n).length===0?null:n}function $T(t,n){return n.map(e=>e(t))}function uH(t){return!t.validate}function GT(t){return t.map(n=>uH(n)?n:e=>n.validate(e))}function WT(t){if(!t)return null;let n=t.filter(HT);return n.length==0?null:function(e){return zT($T(e,n))}}function Sb(t){return t!=null?WT(GT(t)):null}function qT(t){if(!t)return null;let n=t.filter(HT);return n.length==0?null:function(e){let i=$T(e,n).map(UT);return Cl(i).pipe(ce(zT))}}function Ib(t){return t!=null?qT(GT(t)):null}function RT(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function YT(t){return t._rawValidators}function QT(t){return t._rawAsyncValidators}function Cb(t){return t?Array.isArray(t)?t:[t]:[]}function uh(t,n){return Array.isArray(t)?t.includes(n):t===n}function kT(t,n){let e=Cb(n);return Cb(t).forEach(r=>{uh(e,r)||e.push(r)}),e}function OT(t,n){return Cb(n).filter(e=>!uh(t,e))}var fh=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Sb(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Ib(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},hi=class extends fh{name;get formDirective(){return null}get path(){return null}},mi=class extends fh{_parent=null;name=null;valueAccessor=null},ph=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Hn=(()=>{class t extends ph{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ee(mi,2))};static \u0275dir=E({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&Y("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[te]})}return t})(),$i=(()=>{class t extends ph{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ee(hi,10))};static \u0275dir=E({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&Y("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[te]})}return t})();var cd="VALID",ch="INVALID",Us="PENDING",dd="DISABLED",fo=class{},hh=class extends fo{value;source;constructor(n,e){super(),this.value=n,this.source=e}},fd=class extends fo{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},pd=class extends fo{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},zs=class extends fo{status;source;constructor(n,e){super(),this.status=n,this.source=e}},mh=class extends fo{source;constructor(n){super(),this.source=n}},md=class extends fo{source;constructor(n){super(),this.source=n}};function Mb(t){return(bh(t)?t.validators:t)||null}function fH(t){return Array.isArray(t)?Sb(t):t||null}function Tb(t,n){return(bh(n)?n.asyncValidators:t)||null}function pH(t){return Array.isArray(t)?Ib(t):t||null}function bh(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function ZT(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new k(1e3,"");if(!i[e])throw new k(1001,"")}function KT(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new k(-1002,"")})}var $s=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Le(this.statusReactive)}set status(n){Le(()=>this.statusReactive.set(n))}_status=xt(()=>this.statusReactive());statusReactive=I(void 0);get valid(){return this.status===cd}get invalid(){return this.status===ch}get pending(){return this.status===Us}get disabled(){return this.status===dd}get enabled(){return this.status!==dd}errors;get pristine(){return Le(this.pristineReactive)}set pristine(n){Le(()=>this.pristineReactive.set(n))}_pristine=xt(()=>this.pristineReactive());pristineReactive=I(!0);get dirty(){return!this.pristine}get touched(){return Le(this.touchedReactive)}set touched(n){Le(()=>this.touchedReactive.set(n))}_touched=xt(()=>this.touchedReactive());touchedReactive=I(!1);get untouched(){return!this.touched}_events=new S;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(kT(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(kT(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(OT(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(OT(n,this._rawAsyncValidators))}hasValidator(n){return uh(this._rawValidators,n)}hasAsyncValidator(n){return uh(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(se(D({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new pd(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new pd(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(se(D({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new fd(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new fd(!0,i))}markAsPending(n={}){this.status=Us;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new zs(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(se(D({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=dd,this.errors=null,this._forEachChild(r=>{r.disable(se(D({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new hh(this.value,i)),this._events.next(new zs(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(se(D({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=cd,this._forEachChild(i=>{i.enable(se(D({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(se(D({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===cd||this.status===Us)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new hh(this.value,e)),this._events.next(new zs(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(se(D({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?dd:cd}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Us,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=UT(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new zs(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new $,this.statusChanges=new $}_calculateStatus(){return this._allControlsDisabled()?dd:this.errors?ch:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Us)?Us:this._anyControlsHaveStatus(ch)?ch:cd}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new fd(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new pd(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){bh(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=fH(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=pH(this._rawAsyncValidators)}},Gs=class extends $s{constructor(n,e,i){super(Mb(e),Tb(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){KT(this,!0,n),Object.keys(n).forEach(i=>{ZT(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,se(D({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new md(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var Db=class extends Gs{};var vd=new y("",{factory:()=>Ch}),Ch="always";function XT(t,n){return[...n.path,t]}function gh(t,n,e=Ch){Ab(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),mH(t,n),vH(t,n),gH(t,n),hH(t,n)}function NT(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),_h(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function vh(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function hH(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function Ab(t,n){let e=YT(t);n.validator!==null?t.setValidators(RT(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=QT(t);n.asyncValidator!==null?t.setAsyncValidators(RT(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();vh(n._rawValidators,r),vh(n._rawAsyncValidators,r)}function _h(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=YT(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=QT(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return vh(n._rawValidators,i),vh(n._rawAsyncValidators,i),e}function mH(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&JT(t,n)})}function gH(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&JT(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function JT(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function vH(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function eA(t,n){t==null,Ab(t,n)}function _H(t,n){return _h(t,n)}function tA(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function yH(t){return Object.getPrototypeOf(t.constructor)===JB}function nA(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function iA(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Bn?e=o:yH(o)?i=o:r=o}),r||i||e||null}function bH(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var CH={provide:hi,useExisting:on(()=>_d)},ud=Promise.resolve(),_d=(()=>{class t extends hi{callSetDisabledState;get submitted(){return Le(this.submittedReactive)}_submitted=xt(()=>this.submittedReactive());submittedReactive=I(!1);_directives=new Set;form;ngSubmit=new $;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Gs({},Sb(e),Ib(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){ud.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),gh(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){ud.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){ud.then(()=>{let i=this._findContainer(e.path),r=new Gs({});eA(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){ud.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){ud.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),nA(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new mh(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ee(va,10),ee(yh,10),ee(vd,8))};static \u0275dir=E({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&G("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_e([CH]),te]})}return t})();function PT(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function FT(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var hd=class extends $s{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(Mb(e),Tb(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),bh(e)&&(e.nonNullable||e.initialValueIsDefault)&&(FT(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new md(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){PT(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){PT(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){FT(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var DH=t=>t instanceof hd;var wH={provide:mi,useExisting:on(()=>Rb)},LT=Promise.resolve(),Rb=(()=>{class t extends mi{_changeDetectorRef;callSetDisabledState;control=new hd;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new $;constructor(e,i,r,o,a,s){super(),this._changeDetectorRef=a,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=iA(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),tA(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){gh(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){LT.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&B(i);LT.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?XT(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(ee(hi,9),ee(va,10),ee(yh,10),ee(gd,10),ee(Re,8),ee(vd,8))};static \u0275dir=E({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[_e([wH]),te,Ae]})}return t})();var Gi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var wb=class extends $s{constructor(n,e,i){super(Mb(e),Tb(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){KT(this,!1,n),n.forEach((i,r)=>{ZT(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],se(D({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new md(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var EH=(()=>{class t extends hi{callSetDisabledState;get submitted(){return Le(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=xt(()=>this._submittedReactive());_submittedReactive=I(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(_h(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return gh(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){NT(e.control||null,e,!1),bH(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,nA(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new mh(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(NT(i||null,e),DH(r)&&(gh(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);eA(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&_H(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Ab(this.form,this),this._oldForm&&_h(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ee(va,10),ee(yh,10),ee(vd,8))};static \u0275dir=E({type:t,features:[te,Ae]})}return t})();var rA=new y("");var xH={provide:mi,useExisting:on(()=>gi)},gi=(()=>{class t extends mi{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new $;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,a){super(),this._ngModelWarningConfig=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=iA(this,o)}ngOnChanges(e){this._added||this._setUpControl(),tA(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return XT(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(ee(hi,13),ee(va,10),ee(yh,10),ee(gd,10),ee(rA,8))};static \u0275dir=E({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[_e([xH]),te,Ae]})}return t})();var SH={provide:hi,useExisting:on(()=>nn)},nn=(()=>{class t extends EH{form=null;ngSubmit=new $;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&G("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_e([SH]),te]})}return t})();var IH=(()=>{class t{_validator=dh;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):dh,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,features:[Ae]})}return t})();var MH={provide:va,useExisting:on(()=>vi),multi:!0};var vi=(()=>{class t extends IH{required;inputName="required";normalizeInput=B;createValidator=e=>BT;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&oe("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[_e([MH]),te]})}return t})();var oA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})();function VT(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var TH=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return VT(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Gs(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Db(r,i)}control(e,i,r){let o={};return this.useNonNullable?(VT(i)?o=i:(o.validators=i,o.asyncValidators=r),new hd(e,se(D({},o),{nonNullable:!0}))):new hd(e,i,r)}array(e,i,r){let o=e.map(a=>this._createControl(a));return new wb(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof hd)return e;if(e instanceof $s)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Dh=(()=>{class t extends TH{group(e,i=null){return super.group(e,i)}control(e,i,r){return super.control(e,i,r)}array(e,i,r){return super.array(e,i,r)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),aA=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:vd,useValue:e.callSetDisabledState??Ch}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[oA]})}return t})(),kb=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:rA,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:vd,useValue:e.callSetDisabledState??Ch}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[oA]})}return t})();var sA=(()=>{class t{_animationsDisabled=qe();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&Y("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2,changeDetection:0})}return t})();var AH=["text"],RH=[[["mat-icon"]],"*"],kH=["mat-icon","*"];function OH(t,n){if(t&1&&X(0,"mat-pseudo-checkbox",1),t&2){let e=C();M("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function NH(t,n){if(t&1&&X(0,"mat-pseudo-checkbox",3),t&2){let e=C();M("disabled",e.disabled)}}function PH(t,n){if(t&1&&(m(0,"span",4),b(1),v()),t&2){let e=C();g(),Se("(",e.group.label,")")}}var Eh=new y("MAT_OPTION_PARENT_COMPONENT"),xh=new y("MatOptgroup");var wh=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},xn=(()=>{class t{_element=d(V);_changeDetectorRef=d(Re);_parent=d(Eh,{optional:!0});group=d(xh,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ve).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=I(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new $;_text;_stateChanges=new S;constructor(){let e=d(ft);e.load(jn),e.load(Vs),this._signalDisableRipple=!!this._parent&&Pi(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!St(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new wh(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&et(AH,7),i&2){let o;U(o=z())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&G("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(yt("id",r.id),oe("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),Y("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",B]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:kH,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(De(RH),A(0,OH,1,2,"mat-pseudo-checkbox",1),W(1),m(2,"span",2,0),W(4,1),v(),A(5,NH,1,1,"mat-pseudo-checkbox",3),A(6,PH,2,1,"span",4),X(7,"div",5)),i&2&&(R(r.multiple?0:-1),g(5),R(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),g(),R(r.group&&r.group._inert?6:-1),g(),M("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[sA,lo],styles:[`.mat-mdc-option {
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
`],encapsulation:2,changeDetection:0})}return t})();function Ob(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function Nb(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var Pb=class{_box;_destroyed=new S;_resizeSubject=new S;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new he(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ee(e=>e.some(i=>i.target===n)),uu({bufferSize:1,refCount:!0}),xe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},lA=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(K);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Pb(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var FH=["notch"],LH=["matFormFieldNotchedOutline",""],VH=["*"],cA=["iconPrefixContainer"],dA=["textPrefixContainer"],uA=["iconSuffixContainer"],fA=["textSuffixContainer"],jH=["textField"],BH=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],HH=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function UH(t,n){t&1&&X(0,"span",21)}function zH(t,n){if(t&1&&(m(0,"label",20),W(1,1),A(2,UH,1,0,"span",21),v()),t&2){let e=C(2);M("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),oe("for",e._control.disableAutomaticLabeling?null:e._control.id),g(2),R(!e.hideRequiredMarker&&e._control.required?2:-1)}}function $H(t,n){if(t&1&&A(0,zH,3,5,"label",20),t&2){let e=C();R(e._hasFloatingLabel()?0:-1)}}function GH(t,n){t&1&&X(0,"div",7)}function WH(t,n){}function qH(t,n){if(t&1&&ue(0,WH,0,0,"ng-template",13),t&2){C(2);let e=Pt(1);M("ngTemplateOutlet",e)}}function YH(t,n){if(t&1&&(m(0,"div",9),A(1,qH,1,1,null,13),v()),t&2){let e=C();M("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),g(),R(e._forceDisplayInfixLabel()?-1:1)}}function QH(t,n){t&1&&(m(0,"div",10,2),W(2,2),v())}function ZH(t,n){t&1&&(m(0,"div",11,3),W(2,3),v())}function KH(t,n){}function XH(t,n){if(t&1&&ue(0,KH,0,0,"ng-template",13),t&2){C();let e=Pt(1);M("ngTemplateOutlet",e)}}function JH(t,n){t&1&&(m(0,"div",14,4),W(2,4),v())}function eU(t,n){t&1&&(m(0,"div",15,5),W(2,5),v())}function tU(t,n){t&1&&X(0,"div",16)}function nU(t,n){t&1&&(m(0,"div",18),W(1,6),v())}function iU(t,n){if(t&1&&(m(0,"mat-hint",22),b(1),v()),t&2){let e=C(2);M("id",e._hintLabelId),g(),fe(e.hintLabel)}}function rU(t,n){if(t&1&&(m(0,"div",19),A(1,iU,2,2,"mat-hint",22),W(2,7),X(3,"div",23),W(4,8),v()),t&2){let e=C();g(),R(e.hintLabel?1:-1)}}var ln=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-label"]]})}return t})(),yA=new y("MatError"),zn=(()=>{class t{id=d(Ve).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&yt("id",r.id)},inputs:{id:"id"},features:[_e([{provide:yA,useExisting:t}])]})}return t})(),Un=(()=>{class t{align="start";id=d(Ve).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(yt("id",r.id),oe("align",null),Y("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),oU=new y("MatPrefix");var aU=new y("MatSuffix");var bA=new y("FloatingLabelParent"),pA=(()=>{class t{_elementRef=d(V);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(lA);_ngZone=d(K);_parent=d(bA);_resizeSubscription=new le;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return sU(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&Y("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function sU(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var hA="mdc-line-ripple--active",Sh="mdc-line-ripple--deactivating",mA=(()=>{class t{_elementRef=d(V);_cleanupTransitionEnd;constructor(){let e=d(K),i=d(Ge);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Sh),e.add(hA)}deactivate(){this._elementRef.nativeElement.classList.add(Sh)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Sh);e.propertyName==="opacity"&&r&&i.remove(hA,Sh)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),gA=(()=>{class t{_elementRef=d(V);_ngZone=d(K);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&et(FH,5),i&2){let o;U(o=z())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&Y("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:LH,ngContentSelectors:VH,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(De(),zt(0,"div",1),_t(1,"div",2,0),W(3),At(),zt(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),bd=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t})}return t})();var Cd=new y("MatFormField"),lU=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),vA="fill",cU="auto",_A="fixed",dU="translateY(-50%)",Sn=(()=>{class t{_elementRef=d(V);_changeDetectorRef=d(Re);_platform=d(Ie);_idGenerator=d(Ve);_ngZone=d(K);_defaults=d(lU,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=mc("iconPrefixContainer");_textPrefixContainerSignal=mc("textPrefixContainer");_iconSuffixContainerSignal=mc("iconSuffixContainer");_textSuffixContainerSignal=mc("textSuffixContainer");_prefixSuffixContainers=xt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=LS(ln);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=hr(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||cU}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||vA;this._appearanceSignal.set(i)}_appearanceSignal=I(vA);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||_A}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||_A}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new S;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=qe();constructor(){let e=this._defaults,i=d(Ct);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),ti(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=xt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe($e([void 0,void 0]),ce(()=>[i.errorState,i.userAriaDescribedBy]),du(),Ee(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(xe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Gt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){US({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=xt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${a+s}px`,h=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,_=`var(--mat-mdc-form-field-label-transform, ${dU} translateX(${h}))`,x=a+s+l+c;return[_,x]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Uf(o,r._labelChild,ln,5),Ze(o,bd,5)(o,oU,5)(o,aU,5)(o,yA,5)(o,Un,5)),i&2){$f();let a;U(a=z())&&(r._formFieldControl=a.first),U(a=z())&&(r._prefixChildren=a),U(a=z())&&(r._suffixChildren=a),U(a=z())&&(r._errorChildren=a),U(a=z())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(zf(r._iconPrefixContainerSignal,cA,5)(r._textPrefixContainerSignal,dA,5)(r._iconSuffixContainerSignal,uA,5)(r._textSuffixContainerSignal,fA,5),et(jH,5)(cA,5)(dA,5)(uA,5)(fA,5)(pA,5)(gA,5)(mA,5)),i&2){$f(4);let o;U(o=z())&&(r._textField=o.first),U(o=z())&&(r._iconPrefixContainer=o.first),U(o=z())&&(r._textPrefixContainer=o.first),U(o=z())&&(r._iconSuffixContainer=o.first),U(o=z())&&(r._textSuffixContainer=o.first),U(o=z())&&(r._floatingLabel=o.first),U(o=z())&&(r._notchedOutline=o.first),U(o=z())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&Y("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[_e([{provide:Cd,useExisting:t},{provide:bA,useExisting:t}])],ngContentSelectors:HH,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(De(BH),ue(0,$H,1,1,"ng-template",null,0,cs),m(2,"div",6,1),G("click",function(a){return r._control.onContainerClick(a)}),A(4,GH,1,0,"div",7),m(5,"div",8),A(6,YH,2,2,"div",9),A(7,QH,3,0,"div",10),A(8,ZH,3,0,"div",11),m(9,"div",12),A(10,XH,1,1,null,13),W(11),v(),A(12,JH,3,0,"div",14),A(13,eU,3,0,"div",15),v(),A(14,tU,1,0,"div",16),v(),m(15,"div",17),A(16,nU,2,0,"div",18)(17,rU,5,1,"div",19),v()),i&2){let o;g(2),Y("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),g(2),R(!r._hasOutline()&&!r._control.disabled?4:-1),g(2),R(r._hasOutline()?6:-1),g(),R(r._hasIconPrefix?7:-1),g(),R(r._hasTextPrefix?8:-1),g(2),R(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),g(2),R(r._hasTextSuffix?12:-1),g(),R(r._hasIconSuffix?13:-1),g(),R(r._hasOutline()?-1:14),g(),Y("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();g(),R((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[pA,gA,ps,mA,Un],styles:[`.mdc-text-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var CA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var Fb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[co,CA,xn,ge]})}return t})();var Dd=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new S;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var wd=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ih=class{applyChanges(n,e,i,r,o){n.forEachOperation((a,s,l)=>{let c,u;if(a.previousIndex==null){let f=i(a,s,l);c=e.createEmbeddedView(f.templateRef,f.context,f.index),u=ui.INSERTED}else l==null?(e.remove(s),u=ui.REMOVED):(c=e.get(s),e.move(c,l),u=ui.MOVED);o&&o({context:c?.context,operation:u,record:a})})}detach(){}};var uU=["*"];var fU=new y("MAT_CARD_CONFIG"),yr=(()=>{class t{appearance;constructor(){let e=d(fU,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&Y("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:uU,decls:1,vars:0,template:function(i,r){i&1&&(De(),W(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2,changeDetection:0})}return t})(),br=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var Cr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var Vb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var pU=["mat-internal-form-field",""],hU=["*"],DA=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&Y("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:pU,ngContentSelectors:hU,decls:1,vars:0,template:function(i,r){i&1&&(De(),W(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2,changeDetection:0})}return t})();var po=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ws=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var jb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var wA=new y("MAT_INPUT_VALUE_ACCESSOR");var Bb=new y("CdkAccordion"),EA=(()=>{class t{_stateChanges=new S;_openCloseAllActions=new S;id=d(Ve).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",B]},exportAs:["cdkAccordion"],features:[_e([{provide:Bb,useExisting:t}]),Ae]})}return t})(),xA=(()=>{class t{accordion=d(Bb,{optional:!0,skipSelf:!0});_changeDetectorRef=d(Re);_expansionDispatcher=d(wd);_openCloseAllSubscription=le.EMPTY;closed=new $;opened=new $;destroyed=new $;expandedChange=new $;id=d(Ve).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=I(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",B],disabled:[2,"disabled","disabled",B]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[_e([{provide:Bb,useValue:void 0}])]})}return t})(),SA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})();var mU=["body"],gU=["bodyWrapper"],vU=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],_U=["mat-expansion-panel-header","*","mat-action-row"];function yU(t,n){}var bU=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],CU=["mat-panel-title","mat-panel-description","*"];function DU(t,n){t&1&&(_t(0,"span",1),Ai(),_t(1,"svg",2),zt(2,"path",3),At()())}var Hb=new y("MAT_ACCORDION"),IA=new y("MAT_EXPANSION_PANEL"),wU=(()=>{class t{_template=d(Ne);_expansionPanel=d(IA,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),MA=new y("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),Dr=(()=>{class t extends xA{_viewContainerRef=d(Je);_animationsDisabled=qe();_document=d(j);_ngZone=d(K);_elementRef=d(V);_renderer=d(Ge);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new $;afterCollapse=new $;_inputChanges=new S;accordion=d(Hb,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=d(Ve).getId("mat-expansion-panel-header-");constructor(){super();let e=d(MA,{optional:!0});this._expansionDispatcher=d(wd),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe($e(null),Ee(()=>this.expanded&&!this._portal),Xe(1)).subscribe(()=>{this._portal=new en(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,wU,5),i&2){let a;U(a=z())&&(r._lazyContent=a.first)}},viewQuery:function(i,r){if(i&1&&et(mU,5)(gU,5),i&2){let o;U(o=z())&&(r._body=o.first),U(o=z())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&Y("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",B],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[_e([{provide:Hb,useValue:void 0},{provide:IA,useExisting:t}]),te,Ae],ngContentSelectors:_U,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(De(vU),W(0),m(1,"div",2,0)(3,"div",3,1)(5,"div",4),W(6,1),ue(7,yU,0,0,"ng-template",5),v(),W(8,2),v()()),i&2&&(g(),oe("inert",r.expanded?null:""),g(2),M("id",r.id),oe("aria-labelledby",r._headerId),g(4),M("cdkPortalOutlet",r._portal))},dependencies:[Fn],styles:[`.mat-expansion-panel {
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
`],encapsulation:2,changeDetection:0})}return t})(),TA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-action-row"]],hostAttrs:[1,"mat-action-row"]})}return t})(),wr=(()=>{class t{panel=d(Dr,{host:!0});_element=d(V);_focusMonitor=d(sn);_changeDetectorRef=d(Re);_parentChangeSubscription=le.EMPTY;constructor(){d(ft).load(jn);let e=this.panel,i=d(MA,{optional:!0}),r=d(new an("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(Ee(a=>!!(a.hideToggle||a.togglePosition))):ot;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=Gt(e.opened,e.closed,o,e._inputChanges.pipe(Ee(a=>!!(a.hideToggle||a.disabled||a.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(Ee(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:St(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&G("click",function(){return r._toggle()})("keydown",function(a){return r._keydown(a)}),i&2&&(oe("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),we("height",r._getHeaderHeight()),Y("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:si(e)]},ngContentSelectors:CU,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(De(bU),_t(0,"span",0),W(1),W(2,1),W(3,2),At(),A(4,DU,3,0,"span",1)),i&2&&(Y("mat-content-hide-toggle",!r._showToggle()),g(4),R(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
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
`],encapsulation:2,changeDetection:0})}return t})(),qs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-panel-description"]],hostAttrs:[1,"mat-expansion-panel-header-description"]})}return t})(),ho=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),Ys=(()=>{class t extends EA{_keyManager;_ownHeaders=new Ut;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe($e(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new mr(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,wr,5),i&2){let a;U(a=z())&&(r._headers=a)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&Y("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",B],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[_e([{provide:Hb,useExisting:t}]),te]})}return t})(),Ub=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[SA,Ln,ge]})}return t})();var mo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[rh,Sn,ge]})}return t})();var Ah=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,e){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=e.map(i=>this._trackTile(i))}_trackTile(n){let e=this._findMatchingGap(n.colspan);return this._markTilePosition(e,n),this.columnIndex=e+n.colspan,new zb(this.rowIndex,e)}_findMatchingGap(n){n>this.tracker.length;let e=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}if(e=this.tracker.indexOf(0,this.columnIndex),e==-1){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}i=this._findGapEndIndex(e),this.columnIndex=e+1}while(i-e<n||i==0);return Math.max(e,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let e=n+1;e<this.tracker.length;e++)if(this.tracker[e]!=0)return e;return this.tracker.length}_markTilePosition(n,e){for(let i=0;i<e.colspan;i++)this.tracker[n+i]=e.rowspan}},zb=class{row;col;constructor(n,e){this.row=n,this.col=e}};var AA=["*"];var SU=`.mat-grid-list {
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
`,RA=new y("MAT_GRID_LIST"),IU=(()=>{class t{_element=d(V);_gridList=d(RA,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(e){this._rowspan=Math.round(Hi(e))}get colspan(){return this._colspan}set colspan(e){this._colspan=Math.round(Hi(e))}_setStyle(e,i){this._element.nativeElement.style[e]=i}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&oe("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:AA,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(De(),_t(0,"div",0),W(1),At())},styles:[`.mat-grid-list {
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
`],encapsulation:2,changeDetection:0})}return t})();var MU=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,Ed=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,e,i,r){this._gutterSize=kA(n),this._rows=e.rowCount,this._rowspan=e.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,e){return`(${n}% - (${this._gutterSize} * ${e}))`}getTilePosition(n,e){return e===0?"0":_a(`(${n} + ${this._gutterSize}) * ${e}`)}getTileSize(n,e){return`(${n} * ${e}) + (${e-1} * ${this._gutterSize})`}setStyle(n,e,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,e,r,o)}setColStyles(n,e,i,r){let o=this.getBaseTileSize(i,r),a=this._direction==="rtl"?"right":"left";n._setStyle(a,this.getTilePosition(o,e)),n._setStyle("width",_a(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},$b=class extends Ed{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,e,i,r){super.init(n,e,i,r),this.fixedRowHeight=kA(this.fixedRowHeight),MU.test(this.fixedRowHeight)}setRowStyles(n,e){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,e)),n._setStyle("height",_a(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",_a(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}},Gb=class extends Ed{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,e,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,e)),n._setStyle("paddingTop",_a(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",_a(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(e=>{e._setStyle("marginTop",null),e._setStyle("paddingTop",null)})}_parseRatio(n){let e=n.split(":");e.length,this.rowHeightRatio=parseFloat(e[0])/parseFloat(e[1])}},Wb=class extends Ed{setRowStyles(n,e){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,e)),n._setStyle("height",_a(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}};function _a(t){return`calc(${t})`}function kA(t){return t.match(/([A-Za-z%]+)$/)?t:`${t}px`}var TU="fit",Er=(()=>{class t{_element=d(V);_dir=d(Ct,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(e){this._cols=Math.max(1,Math.round(Hi(e)))}get gutterSize(){return this._gutter}set gutterSize(e){this._gutter=`${e??""}`}get rowHeight(){return this._rowHeight}set rowHeight(e){let i=`${e??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(e){this._tileStyler&&this._tileStyler.reset(this),e===TU?this._tileStyler=new Wb:e&&e.indexOf(":")>-1?this._tileStyler=new Gb(e):this._tileStyler=new $b(e)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new Ah);let e=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,e,this.cols,r),i.forEach((o,a)=>{let s=e.positions[a];this._tileStyler.setStyle(o,s.row,s.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(e){e&&(this._element.nativeElement.style[e[0]]=e[1])}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,IU,5),i&2){let a;U(a=z())&&(r._tiles=a)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&oe("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[_e([{provide:RA,useExisting:t}])],ngContentSelectors:AA,decls:2,vars:0,template:function(i,r){i&1&&(De(),_t(0,"div"),W(1),At())},styles:[SU],encapsulation:2,changeDetection:0})}return t})(),qb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[jb,ge,jb]})}return t})();function OA(t){return Error(`Unable to find icon with the name "${t}"`)}function AU(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function NA(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function PA(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var xr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},LA=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new xr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(Tt.HTML,r);if(!a)throw PA(r);let s=ua(a);return this._addSvgIconConfig(e,i,new xr("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new xr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Tt.HTML,i);if(!o)throw PA(i);let a=ua(o);return this._addSvgIconSetConfig(e,new xr("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Tt.RESOURCE_URL,e);if(!i)throw NA(e);let r=this._cachedIconsByUrl.get(i);return r?Q(kh(r)):this._loadSvgIconFromConfig(new xr(e,null)).pipe(Rt(o=>this._cachedIconsByUrl.set(i,o)),ce(o=>kh(o)))}getNamedSvgIcon(e,i=""){let r=FA(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):bl(OA(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Q(kh(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ce(i=>kh(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Q(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(Fr(s=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(Tt.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(c)),Q(null)})));return Cl(o).pipe(ce(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw OA(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(Rt(i=>e.svgText=i),ce(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Q(null):this._fetchIcon(e).pipe(Rt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(ua("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(ua("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw AU();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(Tt.RESOURCE_URL,i);if(!a)throw NA(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let l=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(ce(c=>ua(c)),Ao(()=>this._inProgressUrlFetches.delete(a)),wl());return this._inProgressUrlFetches.set(a,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(FA(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return RU(o)?new xr(o.url,null,o.options):new xr(o,null)}}static \u0275fac=function(i){return new(i||t)(ne(uy,8),ne(wc),ne(j,8),ne(qt))};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function kh(t){return t.cloneNode(!0)}function FA(t,n){return t+":"+n}function RU(t){return!!(t.url&&t.options)}var kU=["*"],OU=new y("MAT_ICON_DEFAULT_OPTIONS"),NU=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(j),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),VA=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],PU=VA.map(t=>`[${t}]`).join(", "),FU=/^url\(['"]?#(.*?)['"]?\)$/,Wi=(()=>{class t{_elementRef=d(V);_iconRegistry=d(LA);_location=d(NU);_errorHandler=d(qt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=le.EMPTY;constructor(){let e=d(new an("aria-hidden"),{optional:!0}),i=d(OU,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(PU),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)VA.forEach(a=>{let s=i[o],l=s.getAttribute(a),c=l?l.match(FU):null;if(c){let u=r.get(s);u||(u=[],r.set(s,u)),u.push({name:a,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Xe(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(oe("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),Ft(r.color?"mat-"+r.color:""),Y("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",B],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:kU,decls:1,vars:0,template:function(i,r){i&1&&(De(),W(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2,changeDetection:0})}return t})(),ya=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var LU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2,changeDetection:0})}return t})(),VU={passive:!0},jA=(()=>{class t{_platform=d(Ie);_ngZone=d(K);_renderer=d(wt).createRenderer(null,null);_styleLoader=d(ft);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return ot;this._styleLoader.load(LU);let i=wn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new S,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,VU)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=wn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var BA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({})}return t})();var jU=["button","checkbox","file","hidden","image","radio","range","reset","submit"],BU=new y("MAT_INPUT_CONFIG"),qi=(()=>{class t{_elementRef=d(V);_platform=d(Ie);ngControl=d(mi,{optional:!0,self:!0});_autofillMonitor=d(jA);_ngZone=d(K);_formField=d(Cd,{optional:!0});_renderer=d(Ge);_uid=d(Ve).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(BU,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new S;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=hr(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(q.required)??!1}set required(e){this._required=hr(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&fb().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=hr(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>fb().has(e));constructor(){let e=d(_d,{optional:!0}),i=d(nn,{optional:!0}),r=d(po),o=d(wA,{optional:!0,self:!0}),a=this._elementRef.nativeElement,s=a.nodeName.toLowerCase();o?Pi(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ws(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=s==="select",this._isTextarea=s==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&ti(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){jU.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&G("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(yt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),oe("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),Y("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},exportAs:["matInput"],features:[_e([{provide:bd,useExisting:t}]),Ae]})}return t})(),Yb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[mo,mo,BA,ge]})}return t})();var HU=["mat-menu-item",""],UU=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],zU=["mat-icon, [matMenuItemIcon]","*"];function $U(t,n){t&1&&(Ai(),m(0,"svg",2),X(1,"polygon",3),v())}var GU=["*"];function WU(t,n){if(t&1){let e=rt();_t(0,"div",0),Hf("click",function(){je(e);let r=C();return Be(r.closed.emit("click"))})("animationstart",function(r){je(e);let o=C();return Be(o._onAnimationStart(r.animationName))})("animationend",function(r){je(e);let o=C();return Be(o._onAnimationDone(r.animationName))})("animationcancel",function(r){je(e);let o=C();return Be(o._onAnimationDone(r.animationName))}),_t(1,"div",1),W(2),At()()}if(t&2){let e=C();Ft(e._classList),Y("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),yt("id",e.panelId),oe("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var Zb=new y("MAT_MENU_PANEL"),xd=(()=>{class t{_elementRef=d(V);_document=d(j);_focusMonitor=d(sn);_parentMenu=d(Zb,{optional:!0});_changeDetectorRef=d(Re);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new S;_focused=new S;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(ft).load(jn),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&G("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(oe("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),Y("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B]},exportAs:["matMenuItem"],attrs:HU,ngContentSelectors:zU,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(De(UU),W(0),m(1,"span",0),W(2,1),v(),X(3,"div",1),A(4,$U,2,0,":svg:svg",2)),i&2&&(g(3),M("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),g(),R(r._triggersSubmenu?4:-1))},dependencies:[lo],encapsulation:2,changeDetection:0})}return t})();var qU=new y("MatMenuContent");var YU=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),Qb="_mat-menu-enter",Ph="_mat-menu-exit",Zs=(()=>{class t{_elementRef=d(V);_changeDetectorRef=d(Re);_injector=d(J);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=qe();_allItems;_directDescendantItems=new Ut;_classList={};_panelAnimationState="void";_animationDone=new S;_isAnimating=I(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=D({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new $;close=this.closed;panelId=d(Ve).getId("mat-menu-panel-");constructor(){let e=d(YU);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new mr(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe($e(this._directDescendantItems),at(e=>Gt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe($e(this._directDescendantItems),at(i=>Gt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:St(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=lt(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=se(D({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===Ph;(i||e===Qb)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===Qb||e===Ph)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(Ph),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?Qb:Ph)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe($e(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,qU,5)(o,xd,5)(o,xd,4),i&2){let a;U(a=z())&&(r.lazyContent=a.first),U(a=z())&&(r._allItems=a),U(a=z())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&et(Ne,5),i&2){let o;U(o=z())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&oe("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",B],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:B(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[_e([{provide:Zb,useExisting:t}])],ngContentSelectors:GU,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(De(),os(0,WU,3,12,"ng-template"))},styles:[`mat-menu {
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
`],encapsulation:2,changeDetection:0})}return t})(),QU=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(J);return()=>la(t)}});var Qs=new WeakMap,ZU=(()=>{class t{_canHaveBackdrop;_element=d(V);_viewContainerRef=d(Je);_menuItemInstance=d(xd,{optional:!0,self:!0});_dir=d(Ct,{optional:!0});_focusMonitor=d(sn);_ngZone=d(K);_injector=d(J);_scrollStrategy=d(QU);_changeDetectorRef=d(Re);_animationsDisabled=qe();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=le.EMPTY;_menuCloseSubscription=le.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(Zb,{optional:!0});this._parentMaterialMenu=i instanceof Zs?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Qs.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Qs.get(i);Qs.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Zs&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(xe(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Zs&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Xe(1)).subscribe(()=>{i.detach(),Qs.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Qs.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=pr(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Zs&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new fi({positionStrategy:qc(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[a,s],[u,f]=[r,o],p=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let h=this._parentMaterialMenu.items.first;this._parentInnerPadding=h?h._getHostElement().offsetTop:0}p=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=a==="top"?"bottom":"top",c=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:p},{originX:o,originY:l,overlayX:f,overlayY:a,offsetY:p},{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:-p},{originX:o,originY:c,overlayX:f,overlayY:s,offsetY:-p}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Q(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(Ee(a=>this._menuOpen&&a!==this._menuItemInstance)):Q();return Gt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new en(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Qs.get(e)===this}_triggerIsAriaDisabled(){return B(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){Vf()};static \u0275dir=E({type:t})}return t})(),HA=(()=>{class t extends ZU{_cleanupTouchstart;_hoverSubscription=le.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new $;onMenuOpen=this.menuOpened;menuClosed=new $;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(Ge);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{da(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){ca(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&G("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&oe("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[te]})}return t})();var UA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[co,zi,ge,Os]})}return t})();var XU=["trigger"],JU=["panel"],ez=[[["mat-select-trigger"]],"*"],tz=["mat-select-trigger","*"];function nz(t,n){if(t&1&&(m(0,"span",4),b(1),v()),t&2){let e=C();g(),fe(e.placeholder)}}function iz(t,n){t&1&&W(0)}function rz(t,n){if(t&1&&(m(0,"span",11),b(1),v()),t&2){let e=C(2);g(),fe(e.triggerValue)}}function oz(t,n){if(t&1&&(m(0,"span",5),A(1,iz,1,0)(2,rz,2,1,"span",11),v()),t&2){let e=C();g(),R(e.customTrigger?1:2)}}function az(t,n){if(t&1){let e=rt();m(0,"div",12,1),G("keydown",function(r){je(e);let o=C();return Be(o._handleKeydown(r))}),W(2,1),v()}if(t&2){let e=C();Ft(e.panelClass),Y("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),oe("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var sz=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(J);return()=>la(t)}}),lz=new y("MAT_SELECT_CONFIG"),cz=new y("MatSelectTrigger"),Kb=class{source;value;constructor(n,e){this.source=n,this.value=e}},go=(()=>{class t{_viewportRuler=d(Ui);_changeDetectorRef=d(Re);_elementRef=d(V);_dir=d(Ct,{optional:!0});_idGenerator=d(Ve);_renderer=d(Ge);_parentFormField=d(Cd,{optional:!0});ngControl=d(mi,{self:!0,optional:!0});_liveAnnouncer=d(Xc);_defaultOptions=d(lz,{optional:!0});_animationsDisabled=qe();_popoverLocation;_initialized=new S;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=Ob(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=Nb(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Kb(this,e)}_scrollStrategyFactory=d(sz);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new S;_errorStateTracker;stateChanges=new S;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=I(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(q.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Yn(()=>{let e=this.options;return e?e.changes.pipe($e(e),at(()=>Gt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(at(()=>this.optionSelectionChanges))});openedChange=new $;_openedStream=this.openedChange.pipe(Ee(e=>e),ce(()=>{}));_closedStream=this.openedChange.pipe(Ee(e=>!e),ce(()=>{}));selectionChange=new $;valueChange=new $;constructor(){let e=d(po),i=d(_d,{optional:!0}),r=d(nn,{optional:!0}),o=d(new an("tabindex"),{optional:!0}),a=d(Yc,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ws(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Dd(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(xe(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(xe(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe($e(null),xe(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Xe(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&lb(this._trackedModal,"aria-owns",i),dT(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;lb(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!St(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let l=this.selected;l&&s!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!St(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(s?l.select():l.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!St(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ps?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new rd(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Gt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(xe(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Gt(...this.options.map(i=>i._stateChanges)).pipe(xe(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Lt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,cz,5)(o,xn,5)(o,xh,5),i&2){let a;U(a=z())&&(r.customTrigger=a.first),U(a=z())&&(r.options=a),U(a=z())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&et(XU,5)(JU,5)(eh,5),i&2){let o;U(o=z())&&(r.trigger=o.first),U(o=z())&&(r.panel=o.first),U(o=z())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&G("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(oe("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),Y("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:si(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",B],placeholder:"placeholder",required:[2,"required","required",B],multiple:[2,"multiple","multiple",B],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",B],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",si],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",B]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[_e([{provide:bd,useExisting:t},{provide:Eh,useExisting:t}]),Ae],ngContentSelectors:tz,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(De(ez),m(0,"div",2,0),G("click",function(){return r.open()}),m(3,"div",3),A(4,nz,2,1,"span",4)(5,oz,3,1,"span",5),v(),m(6,"div",6)(7,"div",7),Ai(),m(8,"svg",8),X(9,"path",9),v()()()(),ue(10,az,3,16,"ng-template",10),G("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=Pt(1);g(3),oe("id",r._valueId),g(),R(r.empty?4:5),g(6),M("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ps,eh],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2,changeDetection:0})}return t})();var Xb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[zi,Fb,ge,Os,mo,Fb]})}return t})();var dz=["switch"],uz=["*"];function fz(t,n){t&1&&(m(0,"span",11),Ai(),m(1,"svg",13),X(2,"path",14),v(),m(3,"svg",15),X(4,"path",16),v()())}var pz=new y("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),Fh=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},e0=(()=>{class t{_elementRef=d(V);_focusMonitor=d(sn);_changeDetectorRef=d(Re);defaults=d(pz);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new Fh(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=qe();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new $;toggleChange=new $;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(ft).load(jn);let e=d(new an("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(Ve).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new Fh(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&et(dz,5),i&2){let o;U(o=z())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(yt("id",r.id),oe("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),Ft(r.color?"mat-"+r.color:""),Y("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",B],color:"color",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:si(e)],checked:[2,"checked","checked",B],hideIcon:[2,"hideIcon","hideIcon",B],disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[_e([{provide:gd,useExisting:on(()=>t),multi:!0},{provide:va,useExisting:t,multi:!0}]),Ae],ngContentSelectors:uz,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(De(),m(0,"div",1)(1,"button",2,0),G("click",function(){return r._handleClick()}),X(3,"div",3)(4,"span",4),m(5,"span",5)(6,"span",6)(7,"span",7),X(8,"span",8),v(),m(9,"span",9),X(10,"span",10),v(),A(11,fz,5,0,"span",11),v()()(),m(12,"label",12),G("click",function(a){return a.stopPropagation()}),W(13),v()()),i&2){let o=Pt(2);M("labelPosition",r.labelPosition),g(),Y("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),M("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),oe("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),g(9),M("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),g(),R(r.hideIcon?-1:11),g(),M("for",r.buttonId),oe("id",r._labelId)}},dependencies:[lo,DA],styles:[`.mdc-switch {
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
`],encapsulation:2,changeDetection:0})}return t})(),zA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[e0,ge]})}return t})();var mz=[[["caption"]],[["colgroup"],["col"]],"*"],gz=["caption","colgroup, col","*"];function vz(t,n){t&1&&W(0,2)}function _z(t,n){t&1&&(m(0,"thead",0),Qe(1,1),v(),m(2,"tbody",0),Qe(3,2)(4,3),v(),m(5,"tfoot",0),Qe(6,4),v())}function yz(t,n){t&1&&Qe(0,1)(1,2)(2,3)(3,4)}var _i=new y("CDK_TABLE");var jh=(()=>{class t{template=d(Ne);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkCellDef",""]]})}return t})(),Bh=(()=>{class t{template=d(Ne);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkHeaderCellDef",""]]})}return t})(),WA=(()=>{class t{template=d(Ne);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkFooterCellDef",""]]})}return t})(),Ks=(()=>{class t{_table=d(_i,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(e){this._setNameInput(e)}_name;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(e){e!==this._stickyEnd&&(this._stickyEnd=e,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(e){e&&(this._name=e,this.cssClassFriendlyName=e.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkColumnDef",""]],contentQueries:function(i,r,o){if(i&1&&Ze(o,jh,5)(o,Bh,5)(o,WA,5),i&2){let a;U(a=z())&&(r.cell=a.first),U(a=z())&&(r.headerCell=a.first),U(a=z())&&(r.footerCell=a.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",B],stickyEnd:[2,"stickyEnd","stickyEnd",B]}})}return t})(),Vh=class{constructor(n,e){e.nativeElement.classList.add(...n._columnCssClassName)}},qA=(()=>{class t extends Vh{constructor(){super(d(Ks),d(V))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[te]})}return t})();var YA=(()=>{class t extends Vh{constructor(){let e=d(Ks),i=d(V);super(e,i);let r=e._table?._getCellRole();r&&i.nativeElement.setAttribute("role",r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[te]})}return t})();var n0=(()=>{class t{template=d(Ne);_differs=d(ar);columns;_columnsDiffer;constructor(){}ngOnChanges(e){if(!this._columnsDiffer){let i=e.columns&&e.columns.currentValue||[];this._columnsDiffer=this._differs.find(i).create(),this._columnsDiffer.diff(i)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(e){return this instanceof Id?e.headerCell.template:this instanceof i0?e.footerCell.template:e.cell.template}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,features:[Ae]})}return t})(),Id=(()=>{class t extends n0{_table=d(_i,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(Ne),d(ar))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",B]},features:[te,Ae]})}return t})(),i0=(()=>{class t extends n0{_table=d(_i,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(Ne),d(ar))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",B]},features:[te,Ae]})}return t})(),Hh=(()=>{class t extends n0{_table=d(_i,{optional:!0});when;constructor(){super(d(Ne),d(ar))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[te]})}return t})(),ba=(()=>{class t{_viewContainer=d(Je);cells;context;static mostRecentCellOutlet=null;constructor(){t.mostRecentCellOutlet=this}ngOnDestroy(){t.mostRecentCellOutlet===this&&(t.mostRecentCellOutlet=null)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkCellOutlet",""]]})}return t})(),r0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Qe(0,0)},dependencies:[ba],encapsulation:2})}return t})();var o0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Qe(0,0)},dependencies:[ba],encapsulation:2})}return t})(),QA=(()=>{class t{templateRef=d(Ne);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["ng-template","cdkNoDataRow",""]]})}return t})(),$A=["top","bottom","left","right"],t0=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(n=>this._updateCachedSizes(n)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(n,e,i=!0,r=!0,o,a,s){this._isNativeHtmlTable=n,this._stickCellCss=e,this._isBrowser=i,this._needsPositionStickyOnElement=r,this.direction=o,this._positionListener=a,this._tableInjector=s,this._borderCellCss={top:`${e}-border-elem-top`,bottom:`${e}-border-elem-bottom`,left:`${e}-border-elem-left`,right:`${e}-border-elem-right`}}clearStickyPositioning(n,e){(e.includes("left")||e.includes("right"))&&this._removeFromStickyColumnReplayQueue(n);let i=[];for(let r of n)r.nodeType===r.ELEMENT_NODE&&i.push(r,...Array.from(r.children));lt({write:()=>{for(let r of i)this._removeStickyStyle(r,e)}},{injector:this._tableInjector})}updateStickyColumns(n,e,i,r=!0,o=!0){if(!n.length||!this._isBrowser||!(e.some(T=>T)||i.some(T=>T))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let a=n[0],s=a.children.length,l=this.direction==="rtl",c=l?"right":"left",u=l?"left":"right",f=e.lastIndexOf(!0),p=i.indexOf(!0),h,_,x;o&&this._updateStickyColumnReplayQueue({rows:[...n],stickyStartStates:[...e],stickyEndStates:[...i]}),lt({earlyRead:()=>{h=this._getCellWidths(a,r),_=this._getStickyStartColumnPositions(h,e),x=this._getStickyEndColumnPositions(h,i)},write:()=>{for(let T of n)for(let N=0;N<s;N++){let pe=T.children[N];e[N]&&this._addStickyStyle(pe,c,_[N],N===f),i[N]&&this._addStickyStyle(pe,u,x[N],N===p)}this._positionListener&&h.some(T=>!!T)&&(this._positionListener.stickyColumnsUpdated({sizes:f===-1?[]:h.slice(0,f+1).map((T,N)=>e[N]?T:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:p===-1?[]:h.slice(p).map((T,N)=>i[N+p]?T:null).reverse()}))}},{injector:this._tableInjector})}stickRows(n,e,i){if(!this._isBrowser)return;let r=i==="bottom"?n.slice().reverse():n,o=i==="bottom"?e.slice().reverse():e,a=[],s=[],l=[];lt({earlyRead:()=>{for(let c=0,u=0;c<r.length;c++){if(!o[c])continue;a[c]=u;let f=r[c];l[c]=this._isNativeHtmlTable?Array.from(f.children):[f];let p=this._retrieveElementSize(f).height;u+=p,s[c]=p}},write:()=>{let c=o.lastIndexOf(!0);for(let u=0;u<r.length;u++){if(!o[u])continue;let f=a[u],p=u===c;for(let h of l[u])this._addStickyStyle(h,i,f,p)}i==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:s,offsets:a,elements:l}):this._positionListener?.stickyFooterRowsUpdated({sizes:s,offsets:a,elements:l})}},{injector:this._tableInjector})}updateStickyFooterContainer(n,e){this._isNativeHtmlTable&&lt({write:()=>{let i=n.querySelector("tfoot");i&&(e.some(r=>!r)?this._removeStickyStyle(i,["bottom"]):this._addStickyStyle(i,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(n,e){if(!n.classList.contains(this._stickCellCss))return;for(let r of e)n.style[r]="",n.classList.remove(this._borderCellCss[r]);$A.some(r=>e.indexOf(r)===-1&&n.style[r])?n.style.zIndex=this._getCalculatedZIndex(n):(n.style.zIndex="",this._needsPositionStickyOnElement&&(n.style.position=""),n.classList.remove(this._stickCellCss))}_addStickyStyle(n,e,i,r){n.classList.add(this._stickCellCss),r&&n.classList.add(this._borderCellCss[e]),n.style[e]=`${i}px`,n.style.zIndex=this._getCalculatedZIndex(n),this._needsPositionStickyOnElement&&(n.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(n){let e={top:100,bottom:10,left:1,right:1},i=0;for(let r of $A)n.style[r]&&(i+=e[r]);return i?`${i}`:""}_getCellWidths(n,e=!0){if(!e&&this._cachedCellWidths.length)return this._cachedCellWidths;let i=[],r=n.children;for(let o=0;o<r.length;o++){let a=r[o];i.push(this._retrieveElementSize(a).width)}return this._cachedCellWidths=i,i}_getStickyStartColumnPositions(n,e){let i=[],r=0;for(let o=0;o<n.length;o++)e[o]&&(i[o]=r,r+=n[o]);return i}_getStickyEndColumnPositions(n,e){let i=[],r=0;for(let o=n.length;o>0;o--)e[o]&&(i[o]=r,r+=n[o]);return i}_retrieveElementSize(n){let e=this._elemSizeCache.get(n);if(e)return e;let i=n.getBoundingClientRect(),r={width:i.width,height:i.height};return this._resizeObserver&&(this._elemSizeCache.set(n,r),this._resizeObserver.observe(n,{box:"border-box"})),r}_updateStickyColumnReplayQueue(n){this._removeFromStickyColumnReplayQueue(n.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(n)}_removeFromStickyColumnReplayQueue(n){let e=new Set(n);for(let i of this._updatedStickyColumnsParamsToReplay)i.rows=i.rows.filter(r=>!e.has(r));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(i=>!!i.rows.length)}_updateCachedSizes(n){let e=!1;for(let i of n){let r=i.borderBoxSize?.length?{width:i.borderBoxSize[0].inlineSize,height:i.borderBoxSize[0].blockSize}:{width:i.contentRect.width,height:i.contentRect.height};r.width!==this._elemSizeCache.get(i.target)?.width&&bz(i.target)&&(e=!0),this._elemSizeCache.set(i.target,r)}e&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let i of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(i.rows,i.stickyStartStates,i.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function bz(t){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(n=>t.classList.contains(n))}var Sd=new y("STICKY_POSITIONING_LISTENER");var a0=(()=>{class t{viewContainer=d(Je);elementRef=d(V);constructor(){let e=d(_i);e._rowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","rowOutlet",""]]})}return t})(),s0=(()=>{class t{viewContainer=d(Je);elementRef=d(V);constructor(){let e=d(_i);e._headerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","headerRowOutlet",""]]})}return t})(),l0=(()=>{class t{viewContainer=d(Je);elementRef=d(V);constructor(){let e=d(_i);e._footerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","footerRowOutlet",""]]})}return t})(),c0=(()=>{class t{viewContainer=d(Je);elementRef=d(V);constructor(){let e=d(_i);e._noDataRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","noDataRowOutlet",""]]})}return t})(),d0=(()=>{class t{_differs=d(ar);_changeDetectorRef=d(Re);_elementRef=d(V);_dir=d(Ct,{optional:!0});_platform=d(Ie);_viewRepeater;_viewportRuler=d(Ui);_injector=d(J);_virtualScrollViewport=d(SM,{optional:!0,host:!0});_positionListener=d(Sd,{optional:!0})||d(Sd,{optional:!0,skipSelf:!0});_document=d(j);_data;_renderedRange;_onDestroy=new S;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new S;_footerRowStickyUpdates=new S;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let e=this._elementRef.nativeElement.getAttribute("role");return e==="grid"||e==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(e){this._trackByFn=e}_trackByFn;get dataSource(){return this._dataSource}set dataSource(e){this._dataSource!==e&&(this._switchDataSource(e),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new S;_dataStream=new S;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(e){this._multiTemplateDataRows=e,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(e){this._fixedLayout=e,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new $;viewChange=new mt({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){d(new an("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((i,r)=>this.trackBy?this.trackBy(r.dataIndex,r.data):r)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(xe(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new Wp:new Ih,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(e=>{e?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),Gp(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let e=this._dataDiffer.diff(this._renderRows);if(!e){this._updateNoDataRow(),this.contentChanged.next();return}let i=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(e,i,(r,o,a)=>this._getEmbeddedViewArgs(r.item,a),r=>r.item.data,r=>{r.operation===ui.INSERTED&&r.context&&this._renderCellTemplateForItem(r.record.item.rowDef,r.context)}),this._updateRowIndexContext(),e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);o.context.$implicit=r.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(e){this._customColumnDefs.add(e)}removeColumnDef(e){this._customColumnDefs.delete(e)}addRowDef(e){this._customRowDefs.add(e)}removeRowDef(e){this._customRowDefs.delete(e)}addHeaderRowDef(e){this._customHeaderRowDefs.add(e),this._headerRowDefChanged=!0}removeHeaderRowDef(e){this._customHeaderRowDefs.delete(e),this._headerRowDefChanged=!0}addFooterRowDef(e){this._customFooterRowDefs.add(e),this._footerRowDefChanged=!0}removeFooterRowDef(e){this._customFooterRowDefs.delete(e),this._footerRowDefChanged=!0}setNoDataRow(e){this._customNoDataRow=e}updateStickyHeaderRowStyles(){let e=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let r=GA(this._headerRowOutlet,"thead");r&&(r.style.display=e.length?"":"none")}let i=this._headerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["top"]),this._stickyStyler.stickRows(e,i,"top"),this._headerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyFooterRowStyles(){let e=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let r=GA(this._footerRowOutlet,"tfoot");r&&(r.style.display=e.length?"":"none")}let i=this._footerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["bottom"]),this._stickyStyler.stickRows(e,i,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,i),this._footerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyColumnStyles(){let e=this._getRenderedRows(this._headerRowOutlet),i=this._getRenderedRows(this._rowOutlet),r=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...e,...i,...r],["left","right"]),this._stickyColumnStylesNeedReset=!1),e.forEach((o,a)=>{this._addStickyColumnStyles([o],this._headerRowDefs[a])}),this._rowDefs.forEach(o=>{let a=[];for(let s=0;s<i.length;s++)this._renderRows[s].rowDef===o&&a.push(i[s]);this._addStickyColumnStyles(a,o)}),r.forEach((o,a)=>{this._addStickyColumnStyles([o],this._footerRowDefs[a])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(e){this._positionListener?.stickyColumnsUpdated(e)}stickyEndColumnsUpdated(e){this._positionListener?.stickyEndColumnsUpdated(e)}stickyHeaderRowsUpdated(e){this._headerRowStickyUpdates.next(e),this._positionListener?.stickyHeaderRowsUpdated(e)}stickyFooterRowsUpdated(e){this._footerRowStickyUpdates.next(e),this._positionListener?.stickyFooterRowsUpdated(e)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let i=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||i,this._forceRecalculateCellWidths=i,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let e=[],i=Math.min(this._data.length,this._renderedRange.end),r=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<i;o++){let a=this._data[o],s=this._getRenderRowsForData(a,o,r.get(a));this._cachedRenderRowsMap.has(a)||this._cachedRenderRowsMap.set(a,new WeakMap);for(let l=0;l<s.length;l++){let c=s[l],u=this._cachedRenderRowsMap.get(c.data);u.has(c.rowDef)?u.get(c.rowDef).push(c):u.set(c.rowDef,[c]),e.push(c)}}return e}_getRenderRowsForData(e,i,r){return this._getRowDefs(e,i).map(a=>{let s=r&&r.has(a)?r.get(a):[];if(s.length){let l=s.shift();return l.dataIndex=i,l}else return{data:e,rowDef:a,dataIndex:i}})}_cacheColumnDefs(){this._columnDefsByName.clear(),Lh(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(i=>{this._columnDefsByName.has(i.name),this._columnDefsByName.set(i.name,i)})}_cacheRowDefs(){this._headerRowDefs=Lh(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=Lh(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=Lh(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let e=this._rowDefs.filter(i=>!i.when);this._defaultRowDef=e[0]}_renderUpdatedColumns(){let e=(a,s)=>{let l=!!s.getColumnsDiff();return a||l},i=this._rowDefs.reduce(e,!1);i&&this._forceRenderDataRows();let r=this._headerRowDefs.reduce(e,!1);r&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(e,!1);return o&&this._forceRenderFooterRows(),i||r||o}_switchDataSource(e){this._data=[],Gp(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),e||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=e}_observeRenderChanges(){if(!this.dataSource)return;let e;Gp(this.dataSource)?e=this.dataSource.connect(this):Io(this.dataSource)?e=this.dataSource:Array.isArray(this.dataSource)&&(e=Q(this.dataSource)),this._renderChangeSubscription=Ci([e,this.viewChange]).pipe(xe(this._onDestroy)).subscribe(([i,r])=>{this._data=i||[],this._renderedRange=r,this._dataStream.next(i),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((e,i)=>this._renderRow(this._headerRowOutlet,e,i)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((e,i)=>this._renderRow(this._footerRowOutlet,e,i)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(e,i){let r=Array.from(i?.columns||[]).map(s=>{let l=this._columnDefsByName.get(s);return l}),o=r.map(s=>s.sticky),a=r.map(s=>s.stickyEnd);this._stickyStyler.updateStickyColumns(e,o,a,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(e){let i=[];for(let r=0;r<e.viewContainer.length;r++){let o=e.viewContainer.get(r);i.push(o.rootNodes[0])}return i}_getRowDefs(e,i){if(this._rowDefs.length===1)return[this._rowDefs[0]];let r=[];if(this.multiTemplateDataRows)r=this._rowDefs.filter(o=>!o.when||o.when(i,e));else{let o=this._rowDefs.find(a=>a.when&&a.when(i,e))||this._defaultRowDef;o&&r.push(o)}return r.length,r}_getEmbeddedViewArgs(e,i){let r=e.rowDef,o={$implicit:e.data};return{templateRef:r.template,context:o,index:i}}_renderRow(e,i,r,o={}){let a=e.viewContainer.createEmbeddedView(i.template,o,r);return this._renderCellTemplateForItem(i,o),a}_renderCellTemplateForItem(e,i){for(let r of this._getCellTemplates(e))ba.mostRecentCellOutlet&&ba.mostRecentCellOutlet._viewContainer.createEmbeddedView(r,i);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let e=this._rowOutlet.viewContainer;for(let i=0,r=e.length;i<r;i++){let a=e.get(i).context;a.count=r,a.first=i===0,a.last=i===r-1,a.even=i%2===0,a.odd=!a.even,this.multiTemplateDataRows?(a.dataIndex=this._renderRows[i].dataIndex,a.renderIndex=i):a.index=this._renderRows[i].dataIndex}}_getCellTemplates(e){return!e||!e.columns?[]:Array.from(e.columns,i=>{let r=this._columnDefsByName.get(i);return e.extractCellTemplate(r)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let e=(i,r)=>i||r.hasStickyChanged();this._headerRowDefs.reduce(e,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(e,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(e,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let e=this._dir?this._dir.value:"ltr",i=this._injector;this._stickyStyler=new t0(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,e,this,i),(this._dir?this._dir.change:Q()).pipe(xe(this._onDestroy)).subscribe(r=>{this._stickyStyler.direction=r,this.updateStickyColumnStyles()})}_setupVirtualScrolling(e){let i=typeof requestAnimationFrame<"u"?$d:Hd;this.viewChange.next({start:0,end:0}),e.renderedRangeStream.pipe(Ra(0,i),xe(this._onDestroy)).subscribe(this.viewChange),e.attach({dataStream:this._dataStream,measureRangeSize:(r,o)=>this._measureRangeSize(r,o)}),Ci([e.renderedContentOffset,this._headerRowStickyUpdates]).pipe(xe(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let a=0;a<o.elements.length;a++){let s=o.elements[a];if(s){let l=o.offsets[a],c=r!==0?Math.max(r-l,l):-l;for(let u of s)u.style.top=`${-c}px`}}}),Ci([e.renderedContentOffset,this._footerRowStickyUpdates]).pipe(xe(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let a=0;a<o.elements.length;a++){let s=o.elements[a];if(s)for(let l of s)l.style.bottom=`${r+o.offsets[a]}px`}})}_getOwnDefs(e){return e.filter(i=>!i._table||i._table===this)}_updateNoDataRow(){let e=this._customNoDataRow||this._noDataRow;if(!e)return;let i=this._rowOutlet.viewContainer.length===0;if(i===this._isShowingNoDataRow)return;let r=this._noDataRowOutlet.viewContainer;if(i){let o=r.createEmbeddedView(e.templateRef),a=o.rootNodes[0];if(o.rootNodes.length===1&&a?.nodeType===this._document.ELEMENT_NODE){a.setAttribute("role","row"),a.classList.add(...e._contentClassNames);let s=a.querySelectorAll(e._cellSelector);for(let l=0;l<s.length;l++)s[l].classList.add(...e._cellClassNames)}}else r.clear();this._isShowingNoDataRow=i,this._changeDetectorRef.markForCheck()}_measureRangeSize(e,i){if(e.start>=e.end||i!=="vertical")return 0;let r=this.viewChange.value,o=this._rowOutlet.viewContainer;e.start<r.start||e.end>r.end;let a=e.start-r.start,s=e.end-e.start,l,c;for(let p=0;p<s;p++){let h=o.get(p+a);if(h&&h.rootNodes.length){l=c=h.rootNodes[0];break}}for(let p=s-1;p>-1;p--){let h=o.get(p+a);if(h&&h.rootNodes.length){c=h.rootNodes[h.rootNodes.length-1];break}}let u=l?.getBoundingClientRect?.(),f=c?.getBoundingClientRect?.();return u&&f?f.bottom-u.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(i,r,o){if(i&1&&Ze(o,QA,5)(o,Ks,5)(o,Hh,5)(o,Id,5)(o,i0,5),i&2){let a;U(a=z())&&(r._noDataRow=a.first),U(a=z())&&(r._contentColumnDefs=a),U(a=z())&&(r._contentRowDefs=a),U(a=z())&&(r._contentHeaderRowDefs=a),U(a=z())&&(r._contentFooterRowDefs=a)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(i,r){i&2&&Y("cdk-table-fixed-layout",r.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",B],fixedLayout:[2,"fixedLayout","fixedLayout",B],recycleRows:[2,"recycleRows","recycleRows",B]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[_e([{provide:_i,useExisting:t},{provide:Sd,useValue:null}])],ngContentSelectors:gz,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(De(mz),W(0),W(1,1),A(2,vz,1,0),A(3,_z,7,0)(4,yz,4,0)),i&2&&(g(2),R(r._isServer?2:-1),g(),R(r._isNativeHtmlTable?3:4))},dependencies:[s0,a0,c0,l0],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return t})();function Lh(t,n){return t.concat(Array.from(n))}function GA(t,n){let e=n.toUpperCase(),i=t.viewContainer.element.nativeElement;for(;i;){let r=i.nodeType===1?i.nodeName:null;if(r===e)return i;if(r==="TABLE")break;i=i.parentNode}return null}var ZA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[zc]})}return t})();var Cz=["*"];function Dz(t,n){t&1&&W(0)}var u0=(()=>{class t{_elementRef=d(V);constructor(){}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkStepHeader",""]],hostAttrs:["role","tab"]})}return t})(),f0=(()=>{class t{template=d(Ne);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkStepLabel",""]]})}return t})();var Ca={NUMBER:"number",EDIT:"edit",DONE:"done",ERROR:"error"},wz=new y("STEPPER_GLOBAL_OPTIONS"),Uh=(()=>{class t{_stepperOptions;_stepper=d(Xs);_displayDefaultIndicatorType;stepLabel;_childForms;content;stepControl;get interacted(){return this._interacted()}set interacted(e){this._interacted.set(e)}_interacted=I(!1);interactedStream=new $;label;errorMessage;ariaLabel;ariaLabelledby;get state(){return this._state()}set state(e){this._state.set(e)}_state=I(void 0);get editable(){return this._editable()}set editable(e){this._editable.set(e)}_editable=I(!0);optional=!1;get completed(){let e=this._completedOverride(),i=this._interacted();return e??(i&&(!this.stepControl||this.stepControl.valid))}set completed(e){this._completedOverride.set(e)}_completedOverride=I(null);index=I(-1);isSelected=xt(()=>this._stepper.selectedIndex===this.index());indicatorType=xt(()=>{let e=this.isSelected(),i=this.completed,r=this._state()??Ca.NUMBER,o=this._editable();return this._showError()&&this.hasError&&!e?Ca.ERROR:this._displayDefaultIndicatorType?!i||e?Ca.NUMBER:o?Ca.EDIT:Ca.DONE:i&&!e?Ca.DONE:i&&e?r:o&&e?Ca.EDIT:r});isNavigable=xt(()=>{let e=this.isSelected();return this.completed||e||!this._stepper.linear});get hasError(){let e=this._customError();return e??this._getDefaultError()}set hasError(e){this._customError.set(e)}_customError=I(null);_getDefaultError(){return this.interacted&&!!this.stepControl?.invalid}constructor(){let e=d(wz,{optional:!0});this._stepperOptions=e||{},this._displayDefaultIndicatorType=this._stepperOptions.displayDefaultIndicatorType!==!1}select(){this._stepper.selected=this}reset(){this._interacted.set(!1),this._completedOverride()!=null&&this._completedOverride.set(!1),this._customError()!=null&&this._customError.set(!1),this.stepControl&&(this._childForms?.forEach(e=>e.resetForm?.()),this.stepControl.reset())}ngOnChanges(){this._stepper._stateChanged()}_markAsInteracted(){this._interacted()||(this._interacted.set(!0),this.interactedStream.emit(this))}_showError(){return this._stepperOptions.showError??this._customError()!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["cdk-step"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,f0,5)(o,hi,5),i&2){let a;U(a=z())&&(r.stepLabel=a.first),U(a=z())&&(r._childForms=a)}},viewQuery:function(i,r){if(i&1&&et(Ne,7),i&2){let o;U(o=z())&&(r.content=o.first)}},inputs:{stepControl:"stepControl",label:"label",errorMessage:"errorMessage",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],state:"state",editable:[2,"editable","editable",B],optional:[2,"optional","optional",B],completed:[2,"completed","completed",B],hasError:[2,"hasError","hasError",B]},outputs:{interactedStream:"interacted"},exportAs:["cdkStep"],features:[Ae],ngContentSelectors:Cz,decls:1,vars:0,template:function(i,r){i&1&&(De(),os(0,Dz,1,0,"ng-template"))},encapsulation:2,changeDetection:0})}return t})(),Xs=(()=>{class t{_dir=d(Ct,{optional:!0});_changeDetectorRef=d(Re);_elementRef=d(V);_destroyed=new S;_keyManager;_steps;steps=new Ut;_stepHeader;_sortedHeaders=new Ut;get linear(){return this._linear()}set linear(e){this._linear.set(e)}_linear=I(!1);get selectedIndex(){return this._selectedIndex()}set selectedIndex(e){this._steps?(this._isValidIndex(e),this.selectedIndex!==e&&(this.selected?._markAsInteracted(),!this._anyControlsInvalidOrPending(e)&&(e>=this.selectedIndex||this.steps.toArray()[e].editable)&&this._updateSelectedItemIndex(e))):this._selectedIndex.set(e)}_selectedIndex=I(0);get selected(){return this.steps?this.steps.toArray()[this.selectedIndex]:void 0}set selected(e){this.selectedIndex=e&&this.steps?this.steps.toArray().indexOf(e):-1}selectionChange=new $;selectedIndexChange=new $;_groupId=d(Ve).getId("cdk-stepper-");get orientation(){return this._orientation}set orientation(e){this._orientation=e,this._keyManager&&this._keyManager.withVerticalOrientation(e==="vertical")}_orientation="horizontal";constructor(){}ngAfterContentInit(){this._steps.changes.pipe($e(this._steps),xe(this._destroyed)).subscribe(e=>{this.steps.reset(e.filter(i=>i._stepper===this)),this.steps.forEach((i,r)=>i.index.set(r)),this.steps.notifyOnChanges()})}ngAfterViewInit(){if(this._stepHeader.changes.pipe($e(this._stepHeader),xe(this._destroyed)).subscribe(e=>{this._sortedHeaders.reset(e.toArray().sort((i,r)=>i._elementRef.nativeElement.compareDocumentPosition(r._elementRef.nativeElement)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1)),this._sortedHeaders.notifyOnChanges()}),this._keyManager=new mr(this._sortedHeaders).withWrap().withHomeAndEnd().withVerticalOrientation(this._orientation==="vertical"),this._keyManager.updateActiveItem(this.selectedIndex),(this._dir?this._dir.change:Q()).pipe($e(this._layoutDirection()),xe(this._destroyed)).subscribe(e=>this._keyManager?.withHorizontalOrientation(e)),this._keyManager.updateActiveItem(this.selectedIndex),this.steps.changes.subscribe(()=>{this.selected||this._selectedIndex.set(Math.max(this.selectedIndex-1,0))}),this._isValidIndex(this.selectedIndex)||this._selectedIndex.set(0),this.linear&&this.selectedIndex>0){let e=this.steps.toArray().slice(0,this._selectedIndex());for(let i of e)i._markAsInteracted()}}ngOnDestroy(){this._keyManager?.destroy(),this.steps.destroy(),this._sortedHeaders.destroy(),this._destroyed.next(),this._destroyed.complete()}next(){this.selectedIndex=Math.min(this._selectedIndex()+1,this.steps.length-1)}previous(){this.selectedIndex=Math.max(this._selectedIndex()-1,0)}reset(){this._updateSelectedItemIndex(0),this.steps.forEach(e=>e.reset()),this._stateChanged()}_getStepLabelId(e){return`${this._groupId}-label-${e}`}_getStepContentId(e){return`${this._groupId}-content-${e}`}_stateChanged(){this._changeDetectorRef.markForCheck()}_getAnimationDirection(e){let i=e-this._selectedIndex();return i<0?this._layoutDirection()==="rtl"?"next":"previous":i>0?this._layoutDirection()==="rtl"?"previous":"next":"current"}_getFocusIndex(){return this._keyManager?this._keyManager.activeItemIndex:this._selectedIndex()}_updateSelectedItemIndex(e){let i=this.steps.toArray(),r=this._selectedIndex();this.selectionChange.emit({selectedIndex:e,previouslySelectedIndex:r,selectedStep:i[e],previouslySelectedStep:i[r]}),this._keyManager&&(this._containsFocus()?this._keyManager.setActiveItem(e):this._keyManager.updateActiveItem(e)),this._selectedIndex.set(e),this.selectedIndexChange.emit(e),this._stateChanged()}_onKeydown(e){let i=St(e),r=e.keyCode,o=this._keyManager;o?.activeItemIndex!=null&&!i&&(r===32||r===13)?(this.selectedIndex=o.activeItemIndex,e.preventDefault()):o?.setFocusOrigin("keyboard").onKeydown(e)}_anyControlsInvalidOrPending(e){return this.linear&&e>=0?this.steps.toArray().slice(0,e).some(i=>{let r=i.stepControl;return(r?r.invalid||r.pending||!i.interacted:!i.completed)&&!i.optional&&!i._completedOverride()}):!1}_layoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_containsFocus(){let e=this._elementRef.nativeElement,i=ra();return e===i||e.contains(i)}_isValidIndex(e){return e>-1&&(!this.steps||e<this.steps.length)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["","cdkStepper",""]],contentQueries:function(i,r,o){if(i&1&&Ze(o,Uh,5)(o,u0,5),i&2){let a;U(a=z())&&(r._steps=a),U(a=z())&&(r._stepHeader=a)}},inputs:{linear:[2,"linear","linear",B],selectedIndex:[2,"selectedIndex","selectedIndex",si],selected:"selected",orientation:"orientation"},outputs:{selectionChange:"selectionChange",selectedIndexChange:"selectedIndexChange"},exportAs:["cdkStepper"]})}return t})(),KA=(()=>{class t{_stepper=d(Xs);type="submit";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["button","cdkStepperNext",""]],hostVars:1,hostBindings:function(i,r){i&1&&G("click",function(){return r._stepper.next()}),i&2&&yt("type",r.type)},inputs:{type:"type"}})}return t})(),XA=(()=>{class t{_stepper=d(Xs);type="button";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["button","cdkStepperPrevious",""]],hostVars:1,hostBindings:function(i,r){i&1&&G("click",function(){return r._stepper.previous()}),i&2&&yt("type",r.type)},inputs:{type:"type"}})}return t})(),JA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var Ez=(t,n,e)=>({index:t,active:n,optional:e});function xz(t,n){if(t&1&&Qe(0,2),t&2){let e=C();M("ngTemplateOutlet",e.iconOverrides[e.state])("ngTemplateOutletContext",M_(2,Ez,e.index,e.active,e.optional))}}function Sz(t,n){if(t&1&&(m(0,"span",7),b(1),v()),t&2){let e=C(2);g(),fe(e._getDefaultTextForState(e.state))}}function Iz(t,n){if(t&1&&(m(0,"span",8),b(1),v()),t&2){let e=C(3);g(),fe(e._intl.completedLabel)}}function Mz(t,n){if(t&1&&(m(0,"span",8),b(1),v()),t&2){let e=C(3);g(),fe(e._intl.editableLabel)}}function Tz(t,n){if(t&1&&(A(0,Iz,2,1,"span",8)(1,Mz,2,1,"span",8),m(2,"mat-icon",7),b(3),v()),t&2){let e=C(2);R(e.state==="done"?0:e.state==="edit"?1:-1),g(3),fe(e._getDefaultTextForState(e.state))}}function Az(t,n){if(t&1&&A(0,Sz,2,1,"span",7)(1,Tz,4,2),t&2){let e,i=C();R((e=i.state)==="number"?0:1)}}function Rz(t,n){t&1&&(m(0,"div",4),Qe(1,9),v()),t&2&&(g(),M("ngTemplateOutlet",n.template))}function kz(t,n){if(t&1&&(m(0,"div",4),b(1),v()),t&2){let e=C();g(),fe(e.label)}}function Oz(t,n){if(t&1&&(m(0,"div",5),b(1),v()),t&2){let e=C();g(),fe(e._intl.optionalLabel)}}function Nz(t,n){if(t&1&&(m(0,"div",6),b(1),v()),t&2){let e=C();g(),fe(e.errorMessage)}}var eR=["*"];function Pz(t,n){}function Fz(t,n){if(t&1&&(W(0),ue(1,Pz,0,0,"ng-template",0)),t&2){let e=C();g(),M("cdkPortalOutlet",e._portal)}}var Lz=["animatedContainer"],tR=t=>({steps:t}),nR=t=>({step:t});function Vz(t,n){t&1&&W(0)}function jz(t,n){if(t&1&&(m(0,"div",5),Qe(1,9)(2,6),v()),t&2){let e=C(2),i=Pt(6);g(),M("ngTemplateOutlet",e.headerPrefix()),g(),M("ngTemplateOutlet",i)("ngTemplateOutletContext",ls(3,tR,e.steps))}}function Bz(t,n){if(t&1&&Qe(0,6),t&2){let e=C(2),i=Pt(6);M("ngTemplateOutlet",i)("ngTemplateOutletContext",ls(2,tR,e.steps))}}function Hz(t,n){if(t&1&&(m(0,"div",10,2),Qe(2,9),v()),t&2){let e=n.$implicit,i=n.$index,r=C(2);Ft("mat-horizontal-stepper-content-"+r._getAnimationDirection(i)),M("id",r._getStepContentId(i)),oe("aria-labelledby",r._getStepLabelId(i))("inert",r.selectedIndex===i?null:""),g(2),M("ngTemplateOutlet",e.content)}}function Uz(t,n){if(t&1&&(m(0,"div",3),A(1,jz,3,5,"div",5)(2,Bz,1,4,"ng-container",6),m(3,"div",7),nt(4,Hz,3,6,"div",8,tt),v()()),t&2){let e=C();g(),R(e.headerPrefix()?1:2),g(3),it(e.steps)}}function zz(t,n){if(t&1&&Qe(0,9),t&2){let e=C(2);M("ngTemplateOutlet",e.headerPrefix())}}function $z(t,n){if(t&1&&(m(0,"div",11),Qe(1,6),m(2,"div",12,2)(4,"div",13)(5,"div",14),Qe(6,9),v()()()()),t&2){let e=n.$implicit,i=n.$index,r=n.$index,o=n.$count,a=C(2),s=Pt(4);g(),M("ngTemplateOutlet",s)("ngTemplateOutletContext",ls(11,nR,e)),g(),Y("mat-stepper-vertical-line",r!==o-1)("mat-vertical-content-container-active",a.selectedIndex===i),oe("inert",a.selectedIndex===i?null:"")("aria-label",a.ariaLabel),g(2),M("id",a._getStepContentId(i)),oe("aria-labelledby",a._getStepLabelId(i)),g(2),M("ngTemplateOutlet",e.content)}}function Gz(t,n){if(t&1&&(m(0,"div",4),A(1,zz,1,1,"ng-container",9),nt(2,$z,7,13,"div",11,tt),v()),t&2){let e=C();g(),R(e.headerPrefix()?1:-1),g(),it(e.steps)}}function Wz(t,n){if(t&1){let e=rt();m(0,"mat-step-header",15),G("click",function(){let r=je(e).step;return Be(r.select())})("keydown",function(r){je(e);let o=C();return Be(o._onKeydown(r))}),v()}if(t&2){let e=n.step,i=C();Y("mat-horizontal-stepper-header",i.orientation==="horizontal")("mat-vertical-stepper-header",i.orientation==="vertical"),M("tabIndex",i._getFocusIndex()===e.index()?0:-1)("id",i._getStepLabelId(e.index()))("index",e.index())("state",e.indicatorType())("label",e.stepLabel||e.label)("selected",e.isSelected())("active",e.isNavigable())("optional",e.optional)("errorMessage",e.errorMessage)("iconOverrides",i._iconOverrides)("disableRipple",i.disableRipple||!e.isNavigable())("color",e.color||i.color),oe("role",i.orientation==="horizontal"?"tab":"button")("aria-posinset",i.orientation==="horizontal"?e.index()+1:null)("aria-setsize",i.orientation==="horizontal"?i.steps.length:null)("aria-selected",i.orientation==="horizontal"?e.isSelected():null)("aria-current",i.orientation==="vertical"&&e.isSelected()?"step":null)("aria-disabled",i.orientation==="vertical"&&e.isSelected()?"true":null)("aria-expanded",i.orientation==="vertical"?e.isSelected():null)("aria-controls",i._getStepContentId(e.index()))("aria-label",e.ariaLabel||null)("aria-labelledby",!e.ariaLabel&&e.ariaLabelledby?e.ariaLabelledby:null)("aria-disabled",e.isNavigable()?null:!0)}}function qz(t,n){t&1&&X(0,"div",17)}function Yz(t,n){if(t&1&&(Qe(0,6),A(1,qz,1,0,"div",17)),t&2){let e=n.$implicit,i=n.$index,r=n.$count;C(2);let o=Pt(4);M("ngTemplateOutlet",o)("ngTemplateOutletContext",ls(3,nR,e)),g(),R(i!==r-1?1:-1)}}function Qz(t,n){if(t&1&&(m(0,"div",16),nt(1,Yz,2,5,null,null,tt),v()),t&2){let e=n.steps,i=C();oe("aria-label",i.ariaLabel),g(),it(e)}}var Md=(()=>{class t extends f0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","matStepLabel",""]],features:[te]})}return t})(),Zz=(()=>{class t{changes=new S;optionalLabel="Optional";completedLabel="Completed";editableLabel="Editable";static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),p0=(()=>{class t extends u0{_intl=d(Zz);_focusMonitor=d(sn);_intlSubscription;state;label;errorMessage;iconOverrides;index;selected=!1;active=!1;optional=!1;disableRipple=!1;color;constructor(){super();let e=d(ft);e.load(jn),e.load(Vs);let i=d(Re);this._intlSubscription=this._intl.changes.subscribe(()=>i.markForCheck())}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){this._intlSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._elementRef)}focus(e,i){e?this._focusMonitor.focusVia(this._elementRef,e,i):this._elementRef.nativeElement.focus(i)}_stringLabel(){return this.label instanceof Md?null:this.label}_templateLabel(){return this.label instanceof Md?this.label:null}_getHostElement(){return this._elementRef.nativeElement}_getDefaultTextForState(e){return e=="number"?`${this.index+1}`:e=="edit"?"create":e=="error"?"warning":e}_hasEmptyLabel(){return!this._stringLabel()&&!this._templateLabel()&&!this._hasOptionalLabel()&&!this._hasErrorLabel()}_hasOptionalLabel(){return this.optional&&this.state!=="error"}_hasErrorLabel(){return this.state==="error"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-step-header"]],hostAttrs:["role","",1,"mat-step-header"],hostVars:4,hostBindings:function(i,r){i&2&&(Ft("mat-"+(r.color||"primary")),Y("mat-step-header-empty-label",r._hasEmptyLabel()))},inputs:{state:"state",label:"label",errorMessage:"errorMessage",iconOverrides:"iconOverrides",index:"index",selected:"selected",active:"active",optional:"optional",disableRipple:"disableRipple",color:"color"},features:[te],decls:10,vars:17,consts:[["matRipple","",1,"mat-step-header-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"],[1,"mat-step-icon-content"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-step-label"],[1,"mat-step-text-label"],[1,"mat-step-optional"],[1,"mat-step-sub-label-error"],["aria-hidden","true"],[1,"cdk-visually-hidden"],[3,"ngTemplateOutlet"]],template:function(i,r){if(i&1&&(X(0,"div",0),m(1,"div")(2,"div",1),A(3,xz,1,6,"ng-container",2)(4,Az,2,1),v()(),m(5,"div",3),A(6,Rz,2,1,"div",4)(7,kz,2,1,"div",4),A(8,Oz,2,1,"div",5),A(9,Nz,2,1,"div",6),v()),i&2){let o;M("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disableRipple),g(),Ft(I_("mat-step-icon-state-",r.state," mat-step-icon")),Y("mat-step-icon-selected",r.selected),g(2),R(r.iconOverrides&&r.iconOverrides[r.state]?3:4),g(2),Y("mat-step-label-active",r.active)("mat-step-label-selected",r.selected)("mat-step-label-error",r.state=="error"),g(),R((o=r._templateLabel())?6:r._stringLabel()?7:-1,o),g(2),R(r._hasOptionalLabel()?8:-1),g(),R(r._hasErrorLabel()?9:-1)}},dependencies:[lo,ps,Wi],styles:[`.mat-step-header {
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
`],encapsulation:2,changeDetection:0})}return t})(),Kz=(()=>{class t{templateRef=d(Ne);name;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["ng-template","matStepperIcon",""]],inputs:{name:[0,"matStepperIcon","name"]}})}return t})(),Xz=(()=>{class t{_template=d(Ne);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["ng-template","matStepContent",""]]})}return t})(),h0=(()=>{class t extends Uh{_errorStateMatcher=d(po,{skipSelf:!0});_viewContainerRef=d(Je);_isSelected=le.EMPTY;stepLabel=void 0;color;_lazyContent;_portal;ngAfterContentInit(){this._isSelected=this._stepper.steps.changes.pipe(at(()=>this._stepper.selectionChange.pipe(ce(e=>e.selectedStep===this),$e(this._stepper.selected===this)))).subscribe(e=>{e&&this._lazyContent&&!this._portal&&(this._portal=new en(this._lazyContent._template,this._viewContainerRef))})}ngOnDestroy(){this._isSelected.unsubscribe()}isErrorState(e,i){let r=this._errorStateMatcher.isErrorState(e,i),o=!!(e&&e.invalid&&this.interacted);return r||o}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-step"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,Md,5)(o,Xz,5),i&2){let a;U(a=z())&&(r.stepLabel=a.first),U(a=z())&&(r._lazyContent=a.first)}},hostAttrs:["hidden",""],inputs:{color:"color"},exportAs:["matStep"],features:[_e([{provide:po,useExisting:t},{provide:Uh,useExisting:t}]),te],ngContentSelectors:eR,decls:1,vars:0,consts:[[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(De(),ue(0,Fz,2,1,"ng-template"))},dependencies:[Fn],encapsulation:2,changeDetection:0})}return t})(),m0=(()=>{class t extends Xs{_ngZone=d(K);_renderer=d(Ge);_animationsDisabled=qe();_cleanupTransition;_isAnimating=I(!1);_stepHeader=void 0;_animatedContainers;_steps=void 0;steps=new Ut;_icons;animationDone=new $;disableRipple=!1;color;labelPosition="end";headerPosition="top";ariaLabel=null;headerPrefix=np(null);_iconOverrides={};get animationDuration(){return this._animationDuration}set animationDuration(e){this._animationDuration=/^\d+$/.test(e)?e+"ms":e}_animationDuration="";_isServer=!d(Ie).isBrowser;constructor(){super();let i=d(V).nativeElement.nodeName.toLowerCase();this.orientation=i==="mat-vertical-stepper"?"vertical":"horizontal"}ngAfterContentInit(){super.ngAfterContentInit(),this._icons.forEach(({name:e,templateRef:i})=>this._iconOverrides[e]=i),this.steps.changes.pipe(xe(this._destroyed)).subscribe(()=>this._stateChanged()),this.selectedIndexChange.pipe(xe(this._destroyed)).subscribe(()=>{let e=this._getAnimationDuration();e==="0ms"||e==="0s"?this._onAnimationDone():this._isAnimating.set(!0)}),this._ngZone.runOutsideAngular(()=>{this._animationsDisabled||setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-stepper-animations-enabled"),this._cleanupTransition=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionend)},200)})}ngAfterViewInit(){if(super.ngAfterViewInit(),typeof queueMicrotask=="function"){let e=!1;this._animatedContainers.changes.pipe($e(null),xe(this._destroyed)).subscribe(()=>queueMicrotask(()=>{e||(e=!0,this.animationDone.emit()),this._stateChanged()}))}}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransition?.()}_getAnimationDuration(){return this._animationsDisabled?"0ms":this.animationDuration?this.animationDuration:this.orientation==="horizontal"?"500ms":"225ms"}_handleTransitionend=e=>{let i=e.target;if(!i)return;let r=this.orientation==="horizontal"&&e.propertyName==="transform"&&i.classList.contains("mat-horizontal-stepper-content-current"),o=this.orientation==="vertical"&&e.propertyName==="grid-template-rows"&&i.classList.contains("mat-vertical-content-container-active");(r||o)&&this._animatedContainers.find(s=>s.nativeElement===i)&&this._onAnimationDone()};_onAnimationDone(){this._isAnimating.set(!1),this.animationDone.emit()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-stepper"],["mat-vertical-stepper"],["mat-horizontal-stepper"],["","matStepper",""]],contentQueries:function(i,r,o){if(i&1&&Ze(o,h0,5)(o,Kz,5),i&2){let a;U(a=z())&&(r._steps=a),U(a=z())&&(r._icons=a)}},viewQuery:function(i,r){if(i&1&&et(p0,5)(Lz,5),i&2){let o;U(o=z())&&(r._stepHeader=o),U(o=z())&&(r._animatedContainers=o)}},hostVars:14,hostBindings:function(i,r){i&2&&(we("--mat-stepper-animation-duration",r._getAnimationDuration()),Y("mat-stepper-horizontal",r.orientation==="horizontal")("mat-stepper-vertical",r.orientation==="vertical")("mat-stepper-label-position-end",r.orientation==="horizontal"&&r.labelPosition=="end")("mat-stepper-label-position-bottom",r.orientation==="horizontal"&&r.labelPosition=="bottom")("mat-stepper-header-position-bottom",r.headerPosition==="bottom")("mat-stepper-animating",r._isAnimating()))},inputs:{disableRipple:"disableRipple",color:"color",labelPosition:"labelPosition",headerPosition:"headerPosition",ariaLabel:[0,"aria-label","ariaLabel"],headerPrefix:[1,"headerPrefix"],animationDuration:"animationDuration"},outputs:{animationDone:"animationDone"},exportAs:["matStepper","matVerticalStepper","matHorizontalStepper"],features:[_e([{provide:Xs,useExisting:t}]),te],ngContentSelectors:eR,decls:7,vars:2,consts:[["stepTemplate",""],["horizontalStepsTemplate",""],["animatedContainer",""],[1,"mat-horizontal-stepper-wrapper"],[1,"mat-vertical-stepper-wrapper"],[1,"mat-horizontal-stepper-header-wrapper"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-horizontal-content-container"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id","class"],[3,"ngTemplateOutlet"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id"],[1,"mat-step"],[1,"mat-vertical-content-container"],["role","region",1,"mat-vertical-stepper-content",3,"id"],[1,"mat-vertical-content"],[3,"click","keydown","tabIndex","id","index","state","label","selected","active","optional","errorMessage","iconOverrides","disableRipple","color"],["aria-orientation","horizontal","role","tablist",1,"mat-horizontal-stepper-header-container"],[1,"mat-stepper-horizontal-line"]],template:function(i,r){if(i&1&&(De(),A(0,Vz,1,0),A(1,Uz,6,1,"div",3)(2,Gz,4,1,"div",4),ue(3,Wz,1,27,"ng-template",null,0,cs)(5,Qz,3,1,"ng-template",null,1,cs)),i&2){let o;R(r._isServer?0:-1),g(),R((o=r.orientation)==="horizontal"?1:o==="vertical"?2:-1)}},dependencies:[ps,p0],styles:[`.mat-stepper-vertical,
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
`],encapsulation:2,changeDetection:0})}return t})(),iR=(()=>{class t extends KA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["button","matStepperNext",""]],hostAttrs:[1,"mat-stepper-next"],hostVars:1,hostBindings:function(i,r){i&2&&yt("type",r.type)},features:[te]})}return t})(),rR=(()=>{class t extends XA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["button","matStepperPrevious",""]],hostAttrs:[1,"mat-stepper-previous"],hostVars:1,hostBindings:function(i,r){i&2&&yt("type",r.type)},features:[te]})}return t})(),oR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({providers:[po],imports:[Ln,JA,ya,co,m0,p0,ge]})}return t})();var e$=[[["caption"]],[["colgroup"],["col"]],"*"],t$=["caption","colgroup, col","*"];function n$(t,n){t&1&&W(0,2)}function i$(t,n){t&1&&(m(0,"thead",0),Qe(1,1),v(),m(2,"tbody",2),Qe(3,3)(4,4),v(),m(5,"tfoot",0),Qe(6,5),v())}function r$(t,n){t&1&&Qe(0,1)(1,3)(2,4)(3,5)}var Js=(()=>{class t extends d0{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(i,r){i&2&&Y("mat-table-fixed-layout",r.fixedLayout)},exportAs:["matTable"],features:[_e([{provide:d0,useExisting:t},{provide:_i,useExisting:t},{provide:Sd,useValue:null}]),te],ngContentSelectors:t$,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(De(e$),W(0),W(1,1),A(2,n$,1,0),A(3,i$,7,0)(4,r$,4,0)),i&2&&(g(2),R(r._isServer?2:-1),g(),R(r._isNativeHtmlTable?3:4))},dependencies:[s0,a0,c0,l0],styles:[`.mat-mdc-table-sticky {
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
`],encapsulation:2})}return t})(),el=(()=>{class t extends jh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","matCellDef",""]],features:[_e([{provide:jh,useExisting:t}]),te]})}return t})(),tl=(()=>{class t extends Bh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","matHeaderCellDef",""]],features:[_e([{provide:Bh,useExisting:t}]),te]})}return t})();var nl=(()=>{class t extends Ks{get name(){return this._name}set name(e){this._setNameInput(e)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[_e([{provide:Ks,useExisting:t}]),te]})}return t})(),il=(()=>{class t extends qA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[te]})}return t})();var rl=(()=>{class t extends YA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[te]})}return t})();var ol=(()=>{class t extends Id{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",B]},features:[_e([{provide:Id,useExisting:t}]),te]})}return t})();var al=(()=>{class t extends Hh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275dir=E({type:t,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[_e([{provide:Hh,useExisting:t}]),te]})}return t})(),sl=(()=>{class t extends r0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[_e([{provide:r0,useExisting:t}]),te],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Qe(0,0)},dependencies:[ba],encapsulation:2})}return t})();var ll=(()=>{class t extends o0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Pe(t)))(r||t)}})();static \u0275cmp=O({type:t,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[_e([{provide:o0,useExisting:t}]),te],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Qe(0,0)},dependencies:[ba],encapsulation:2})}return t})();var aR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ZA,ge]})}return t})();var o$=["*",[["mat-toolbar-row"]]],a$=["*","mat-toolbar-row"],s$=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=E({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),sR=(()=>{class t{_elementRef=d(V);_platform=d(Ie);_document=d(j);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=O({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&Ze(o,s$,5),i&2){let a;U(a=z())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(Ft(r.color?"mat-"+r.color:""),Y("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:a$,decls:2,vars:0,template:function(i,r){i&1&&(De(o$),W(0),W(1,1))},styles:[`.mat-toolbar {
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
`],encapsulation:2,changeDetection:0})}return t})();var lR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ge]})}return t})();var Yi=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[ga,ya,gT,Vb,mo,qb,Xb,Yb,kb,hs]})},vo=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[Ub,ga,ya,aR,hs]})},zh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[Ub,ga,ya,Vb,mo,qb,Xb,Yb,kb,hs]})},$h=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=F({type:t});static \u0275inj=P({imports:[lR,ya,UA,zA,aA]})};var Me=class t{static snackBarConfig(){let n=new uo;return n.duration=12e3,n.panelClass=["btn","btn-outline-dark"],n.verticalPosition="top",n.horizontalPosition="center",n}static openSnackBar(n,e,i){let r=t.snackBarConfig();i.open(n,e,r)}static getMatDialogConf(){let n=new Bs;return n.disableClose=!1,n.autoFocus=!0,n.exitAnimationDuration="1000ms",n.enterAnimationDuration="1000ms",n}static async generateSHA256(n){var e=new TextEncoder;let i=e.encode(n),r=await crypto.subtle.digest("SHA-256",i);return Array.from(new Uint8Array(r)).map(a=>a.toString(16).padStart(2,"0")).join("")}static isSha256(n){return/^[0-9a-fA-F]{64}$/.test(n)}};var L=class{static PRODUCTOS_ID="productoList";static USUARIOS_ID="usuarioList";static LOTES_INVENTARIO_ID="loteList";static PEDIDOS_ID="pedidoList";static DETALLE_PEDIDOS_ID="detallePedidoList";static MOVIMIENTOS_INVENTARIO_ID="movimientoInventarioList";static ESTADO_PEDIDO_ID="estadoPedidoList";static ESTADO_PRODUCTO_ID="estadoProductoList";static TIPO_MOVIMIENTO_ID="tipoMovimientoList";static PASARELA_ID="pasarelaList";static LOGGED_USUARIO="usuario";static LOGGED="logged";static estadoPedidoVacio(){return{id:0,descripcion:""}}static estadoProductoVacio(){return{id:0,descripcion:""}}static tipoMovimientoVacio(){return{id:0,descripcion:""}}static usuarioVacio(){return{id:0,usuario:"",password:"",nombres:"",apellidos:"",fecha_creacion:new Date}}static productoVacio(){return{id:0,sku:"",nombre:"",descripcion:"",precio_venta:0,estado:this.estadoProductoVacio()}}static loteInventarioVacio(){return{id:0,producto:this.productoVacio(),cantidad_inicial:0,cantidad_actual:0,costo_unitario:0,fecha_ingreso:new Date}}static pedidoVacio(){return{id:0,usuario:this.usuarioVacio(),total:0,estado:this.estadoPedidoVacio()}}static detallePedidoVacio(){return{id:0,pedido:this.pedidoVacio(),producto:this.productoVacio(),cantidad:0,precio_unitario_venta:0}}static movimientoInventarioVacio(){return{id:0,producto:this.productoVacio(),lote:this.loteInventarioVacio(),tipo_movimiento:this.tipoMovimientoVacio(),cantidad:0,pedido:this.pedidoVacio(),fecha:new Date}}static pasarelaVacio(){return{id:0,nombre:"",descripcion:"",comision:0}}static getUsuarios(){var n=[],e="";return Me.generateSHA256("1234").then(i=>e=i),n.push({id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}),n.push({id:2,usuario:"jamileth",password:e,nombres:"Jamileth",apellidos:"Martinez",fecha_creacion:new Date}),n.push({id:3,usuario:"rubix",password:e,nombres:"Rubi",apellidos:"Mejia",fecha_creacion:new Date}),n.push({id:4,usuario:"ale",password:e,nombres:"Alejandra",apellidos:"Guardado",fecha_creacion:new Date}),n.push({id:5,usuario:"khaysernberg",password:e,nombres:"Cesar",apellidos:"Gomez",fecha_creacion:new Date}),n.push({id:6,usuario:"miriam",password:e,nombres:"Mirian",apellidos:"Rivas",fecha_creacion:new Date}),n}static getEstadosPedido(){var n=[];return n.push({id:1,descripcion:"Carrito"}),n.push({id:2,descripcion:"Apartado"}),n.push({id:3,descripcion:"Pagado"}),n}static getEstadosProducto(){var n=[];return n.push({id:1,descripcion:"Disponible"}),n.push({id:2,descripcion:"NoDisponible"}),n.push({id:3,descripcion:"Agotado"}),n}static getTiposMovimiento(){var n=[];return n.push({id:1,descripcion:"Compra"}),n.push({id:2,descripcion:"Venta"}),n.push({id:3,descripcion:"Merma"}),n.push({id:4,descripcion:"DevolucionCompra"}),n.push({id:5,descripcion:"DevolucionVenta"}),n}static getProductos(){var n=[],e=this.getEstadosProducto();return n.push({id:1,sku:"p1",nombre:"producto1",descripcion:"descripcion1",precio_venta:1.5,estado:e[0],stock_local:25,stock_web:125}),n.push({id:2,sku:"p2",nombre:"producto2",descripcion:"descripcion2",precio_venta:2.5,estado:e[0],stock_local:25,stock_web:50}),n.push({id:3,sku:"p3",nombre:"producto3",descripcion:"descripcion3",precio_venta:3.5,estado:e[0],stock_local:25,stock_web:125}),n}static getLotesInventario(){var n=[],e=this.getProductos();return n.push({id:1,producto:e[0],cantidad_inicial:100,cantidad_actual:100,costo_unitario:.9,fecha_ingreso:new Date}),n.push({id:2,producto:e[0],cantidad_inicial:50,cantidad_actual:50,costo_unitario:.95,fecha_ingreso:new Date}),n.push({id:3,producto:e[1],cantidad_inicial:75,cantidad_actual:75,costo_unitario:2,fecha_ingreso:new Date}),n.push({id:4,producto:e[2],cantidad_inicial:150,cantidad_actual:150,costo_unitario:2.75,fecha_ingreso:new Date}),n}static getPedidos(){var n=[],e=this.getUsuarios(),i=this.getEstadosPedido();return n.push({id:1,usuario:e[1],total:10,estado:i[0]}),n.push({id:2,usuario:e[2],total:10,estado:i[0]}),n.push({id:3,usuario:e[3],total:10,estado:i[2]}),n}static getDetallePedidos(){var n=[],e=this.getProductos(),i=this.getPedidos();return n.push({id:1,pedido:i[0],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:2,pedido:i[0],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:3,pedido:i[1],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:4,pedido:i[1],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:5,pedido:i[2],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:6,pedido:i[2],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n}static getMovimientosInventario(){var n=[],e=this.getProductos(),i=this.getLotesInventario(),r=this.getTiposMovimiento();return n.push({id:1,producto:e[0],lote:i[0],tipo_movimiento:r[0],cantidad:100,pedido:null,fecha:new Date}),n.push({id:2,producto:e[0],lote:i[1],tipo_movimiento:r[0],cantidad:50,pedido:null,fecha:new Date}),n.push({id:3,producto:e[1],lote:i[2],tipo_movimiento:r[0],cantidad:75,pedido:null,fecha:new Date}),n.push({id:4,producto:e[2],lote:i[3],tipo_movimiento:r[0],cantidad:150,pedido:null,fecha:new Date}),n}static getPasarelas(){var n=[];return n.push({id:1,nombre:"Wompi",descripcion:"wompi el salvador",comision:1.5}),n.push({id:2,nombre:"Serfinsa",descripcion:"serfinsa el salvador",comision:1.5}),n.push({id:3,nombre:"PayPal",descripcion:"PayPal international",comision:1.5}),n}};var c$=new y("WindowLocalStorage",{providedIn:"root",factory:()=>{let t=d(ir);return sp(t)?localStorage:{length:0,clear:()=>{},getItem:()=>null,key:()=>null,removeItem:()=>{},setItem:()=>{}}}}),cn=class t{storage=d(c$);setItem(n,e){let i=typeof e=="string"?e:JSON.stringify(e);this.storage.setItem(n,i)}getItem(n){let e=this.storage.getItem(n);if(!e)return null;try{return JSON.parse(e)}catch{return e}}removeItem(n){this.storage.removeItem(n)}clear(){this.storage.clear()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Vt=class t{localStorageService=d(cn);constructor(){}getUsuarios(){var n=[],e=this.localStorageService.getItem(L.USUARIOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getUsuarios(),this.localStorageService.setItem(L.USUARIOS_ID,n)),n}getEstadosPedido(){var n=[],e=this.localStorageService.getItem(L.ESTADO_PEDIDO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getEstadosPedido(),this.localStorageService.setItem(L.ESTADO_PEDIDO_ID,n)),n}getEstadosProducto(){var n=[],e=this.localStorageService.getItem(L.ESTADO_PRODUCTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getEstadosProducto(),this.localStorageService.setItem(L.ESTADO_PRODUCTO_ID,n)),n}getTiposMovimiento(){var n=[],e=this.localStorageService.getItem(L.TIPO_MOVIMIENTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getTiposMovimiento(),this.localStorageService.setItem(L.TIPO_MOVIMIENTO_ID,n)),n}getProductos(){var n=[],e=this.localStorageService.getItem(L.PRODUCTOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getProductos(),this.localStorageService.setItem(L.PRODUCTOS_ID,n)),n}getLotesInventario(){var n=[],e=this.localStorageService.getItem(L.LOTES_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getLotesInventario(),this.localStorageService.setItem(L.LOTES_INVENTARIO_ID,n)),n}getPedidos(){var n=[],e=this.localStorageService.getItem(L.PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getPedidos(),this.localStorageService.setItem(L.PEDIDOS_ID,n)),n}getDetallePedidos(){var n=[],e=this.localStorageService.getItem(L.DETALLE_PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getDetallePedidos(),this.localStorageService.setItem(L.DETALLE_PEDIDOS_ID,n)),n}getMovimientosInventario(){var n=[],e=this.localStorageService.getItem(L.MOVIMIENTOS_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=L.getMovimientosInventario(),this.localStorageService.setItem(L.MOVIMIENTOS_INVENTARIO_ID,n)),n}getPasarelas(){var n=[],e=this.localStorageService.getItem(L.PASARELA_ID);return e&&(n=e,this.localStorageService.setItem(L.PASARELA_ID,n)),(!n||n.length==0)&&(n=L.getPasarelas()),n}pushProducto(n){var e=this.getProductos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.PRODUCTOS_ID,e)}pushLoteInventario(n){var e=this.getLotesInventario();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.LOTES_INVENTARIO_ID,e)}pushPedido(n){var e=this.getPedidos();return n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.PEDIDOS_ID,e),n}pushDetallePedido(n){var e=this.getDetallePedidos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.DETALLE_PEDIDOS_ID,e)}pushMovimientoInventario(n){var e=this.getMovimientosInventario();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.MOVIMIENTOS_INVENTARIO_ID,e)}editarProducto(n){var e=this.getProductos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.PRODUCTOS_ID,i)}editarLoteInventario(n){var e=this.getLotesInventario(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.LOTES_INVENTARIO_ID,i)}editarDetallePedido(n){var e=this.getDetallePedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.DETALLE_PEDIDOS_ID,i)}editarPedido(n){var e=this.getPedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(L.PEDIDOS_ID,i)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var $n=class t{dataService=d(Vt);_snackBar=d(En);constructor(){}getPedidosUsuario(n){var e=[],i=this.dataService.getPedidos();if(i.length>0)for(let r of i)r.usuario.id==n.id&&r.estado.id<3&&e.push(r);return e}getUltimoPedidoUsuario(n){var e=L.pedidoVacio(),i=this.getPedidosUsuario(n);if(i.length>0)e=i[i.length-1];else{let r=this.dataService.getEstadosPedido();e.usuario=n,e.total=0,e.estado=r[r.findIndex(o=>o.id==1)],e=this.dataService.pushPedido(e)}return e}getDetallesPedido(n){var e=[],i=this.dataService.getDetallePedidos();if(i.length>0)for(let r of i)r.pedido.id==n.id&&e.push(r);return e}aumentarDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&(i.cantidad++,this.dataService.editarDetallePedido(i));return e}disminuirDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&i.cantidad>0&&(i.cantidad--,this.dataService.editarDetallePedido(i));return e}pedidoPagado(n){var e=this.dataService.getEstadosPedido();n.estado=e[e.findIndex(r=>r.id==3)],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)(r.pedido.id=n.id)&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r,1))}ventaPagada(n){var e=this.dataService.getEstadosPedido();n.estado=e[e.findIndex(r=>r.id==3)],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)(r.pedido.id=n.id)&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r,2))}registrarPagoInventario(n,e){let i=L.movimientoInventarioVacio();i.cantidad=n.cantidad,i.fecha=new Date,i.pedido=n.pedido,i.producto=n.producto;let r=this.dataService.getTiposMovimiento();i.tipo_movimiento=r[r.findIndex(l=>l.id==2)];let o=this.dataService.getLotesInventario(),a=[],s=0;s=n.cantidad;for(let l of o)l.producto.id==n.producto.id&&l.cantidad_actual>0&&s>0&&(a.push(l),l.cantidad_actual<s&&(s-=l.cantidad_actual));s=n.cantidad;for(let l of a)if(s>0){l.cantidad_actual<s?(s-=l.cantidad_actual,l.cantidad_actual=0):(l.cantidad_actual-=s,s=0);let c=i;c.lote=l,this.dataService.editarLoteInventario(l),this.dataService.pushMovimientoInventario(c)}e==1&&n.producto.stock_web&&n.producto.stock_web>0&&(n.producto.stock_web-=n.cantidad,this.dataService.editarProducto(n.producto)),e==2&&n.producto.stock_local&&n.producto.stock_local>0&&(n.producto.stock_local-=n.cantidad,this.dataService.editarProducto(n.producto))}validarExistenciaInventario(n){let e=this.dataService.getLotesInventario(),i=[],r=0;r=n.cantidad;for(let o of e)o.producto.id==n.producto.id&&o.cantidad_actual>0&&r>0&&(i.push(o),o.cantidad_actual<r?r-=o.cantidad_actual:r=0);return i&&i.length>0&&r==0}getProductosStocWeb(){let n=this.dataService.getProductos(),e=[];if(n.length>0)for(let i of n)i.estado.id==1&&i.stock_web&&i.stock_web>0&&e.push(i);return e}getProductosStocLocal(){let n=this.dataService.getProductos(),e=[];if(n.length>0)for(let i of n)i.estado.id==1&&i.stock_local&&i.stock_local>0&&e.push(i);return e}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Gn=class t{formBuilder=d(Dh);validateFormControls(n){var e=n.valid,i=n.getRawValue();for(let r=0;r<i.length;r++)i[r]?.enabled&&!i[r]?.valid&&(e=!1);return e}getFormGroup(n){var e={};return n&&n.length>0&&n.forEach(i=>e[i.name]=[i.value||"",i.validators||[]]),this.formBuilder.group(e)}newProductoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.descripcion,value:i})),[{id:1,name:"sku",label:"Codigo",type:"text",controlType:"input",validators:[q.required,q.minLength(3)]},{id:2,name:"nombre",label:"Nombre Producto",type:"text",controlType:"input",validators:[q.required,q.minLength(5)]},{id:3,name:"descripcion",label:"Descripcion",type:"text",controlType:"input",validators:[q.required,q.minLength(5)]},{id:4,name:"precio_venta",value:0,label:"Precio de Venta",type:"number",controlType:"input",validators:[q.required,q.min(.01)]},{id:5,name:"estado",label:"Estado",type:"text",controlType:"select",validators:[q.required],options:e}]}newCompraFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[q.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[q.required,q.min(.01)]},{id:3,name:"pagoTotal",value:0,label:"Pago Total",type:"number",controlType:"input",validators:[q.required,q.min(.01)]}]}newVentaLineaFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[q.required],options:e}]}newVentaLocalFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[q.required],options:e}]}newVentaLocalTipoPagoFormControls(){var n=[];return n.push({id:1,name:"Efectivo",value:1}),n.push({id:2,name:"Tarjeta",value:2}),[{id:1,name:"tipoPago",label:"Forma de Pago",type:"text",controlType:"select",validators:[q.required],options:n}]}newVentaLocalClienteFormControls(){return[{id:1,name:"nombre",label:"Nombre",type:"text",controlType:"input",validators:[q.required,q.minLength(3)]},{id:2,name:"documento",label:"Numero Documento",type:"text",controlType:"input",validators:[q.required,q.minLength(3)]}]}newAgregarDetallePedidoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", Precio: $"+i.precio_venta,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[q.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[q.required,q.min(1)]}]}newAgregarDetallePedidoVentaControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+", Disponible: $"+i.stock_local,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[q.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[q.required,q.min(1)]}]}newMermaForm(){return this.formBuilder.group({sku:["",[q.required,q.minLength(5)]],nombre:["",[q.required,q.minLength(3)]],descripcion:["",[q.required,q.minLength(3)]],precio_venta:[0,[q.required,q.min(.01)]],estado:[L.estadoProductoVacio(),[q.required]]})}newDevolucionCompraForm(){return this.formBuilder.group({sku:["",[q.required,q.minLength(5)]],nombre:["",[q.required,q.minLength(3)]],descripcion:["",[q.required,q.minLength(3)]],precio_venta:[0,[q.required,q.min(.01)]],estado:[L.estadoProductoVacio(),[q.required]]})}newDevolucionVentaForm(){return this.formBuilder.group({sku:["",[q.required,q.minLength(5)]],nombre:["",[q.required,q.minLength(3)]],descripcion:["",[q.required,q.minLength(3)]],precio_venta:[0,[q.required,q.min(.01)]],estado:[L.estadoProductoVacio(),[q.required]]})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var dn=class t{dataService=d(Vt);localStorageService=d(cn);constructor(){}getUsuarioAnonimo(){return{id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}}getUsuarios(){return this.dataService.getUsuarios()}pushUsuario(n){var e=this.getUsuarios();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(L.USUARIOS_ID,e)}editarUsuario(n){var e=this.getUsuarios(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n,Me.isSha256(o.password)||Me.generateSHA256(o.password).then(a=>o.password=a)),i.push(o)}),this.localStorageService.setItem(L.USUARIOS_ID,i)}validarUsuario(n,e){var i="";Me.generateSHA256(e).then(a=>i=a);var r=this.getUsuarios(),o=L.usuarioVacio();return r&&r.length&&r.filter(a=>a.usuario==n).filter(a=>a.password==i).forEach(a=>o=a),o&&o.id>1?o:null}getUsuarioLoggeado(){var n=L.usuarioVacio(),e=this.localStorageService.getItem("usuario");return e&&(n=e),n}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};var Wn=class{isErrorState(n,e){let i=e&&e.submitted;return!!(n&&n.invalid&&(n.dirty||n.touched||i))}};var cR=()=>["producto","cantidad","precio","opciones"];function d$(t,n){t&1&&(m(0,"th",14),b(1,"Producto"),v()),t&2&&we("text-align","center")}function u$(t,n){if(t&1&&(m(0,"td",15),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),ss(" ",e.producto.sku," ",e.producto.nombre," ")}}function f$(t,n){t&1&&(m(0,"th",14),b(1,"Cantidad"),v()),t&2&&we("text-align","center")}function p$(t,n){if(t&1&&(m(0,"td",15),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",e.cantidad)}}function h$(t,n){t&1&&(m(0,"th",14),b(1,"Precio"),v()),t&2&&we("text-align","center")}function m$(t,n){if(t&1&&(m(0,"td",15),b(1),Li(2,"currency"),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",Vi(2,3,e.precio_unitario_venta))}}function g$(t,n){t&1&&(m(0,"th",16),b(1,"OPCIONES"),v())}function v$(t,n){if(t&1){let e=rt();m(0,"td",15)(1,"button",17),G("click",function(){let r=je(e).$implicit,o=C();return Be(o.aumentarDetallePedido(r))}),m(2,"mat-icon"),b(3,"arrow_circle_up"),v()(),m(4,"button",17),G("click",function(){let r=je(e).$implicit,o=C();return Be(o.disminuirDetallePedido(r))}),m(5,"mat-icon"),b(6,"arrow_circle_down"),v()()()}}function _$(t,n){t&1&&X(0,"tr",18)}function y$(t,n){t&1&&X(0,"tr",19)}function b$(t,n){if(t&1&&X(0,"textarea",4),t&2){let e=C().$implicit,i=C();M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function C$(t,n){if(t&1&&(m(0,"mat-option",8),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function D$(t,n){if(t&1){let e=rt();m(0,"mat-select",7),G("selectionChange",function(r){je(e);let o=C(2);return Be(o.onSelectionChange(r))}),nt(1,C$,2,2,"mat-option",8,tt),v()}if(t&2){let e=C().$implicit,i=C();M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function w$(t,n){if(t&1&&X(0,"input",6),t&2){let e=C().$implicit,i=C();M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function E$(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function x$(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function S$(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function I$(t,n){if(t&1&&(m(0,"div")(1,"mat-form-field")(2,"mat-label"),b(3),v(),A(4,b$,1,2,"textarea",4)(5,D$,3,3,"mat-select",5)(6,w$,1,3,"input",6),A(7,E$,2,1,"mat-hint"),A(8,x$,2,1,"mat-error"),A(9,S$,2,1,"mat-error"),v()()),t&2){let e,i,r,o,a=n.$implicit,s=C();g(3),fe(a.label),g(),R((e=a.controlType)==="textarea"?4:e==="select"?5:6),g(3),R((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),g(),R((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),g(),R((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}function M$(t,n){if(t&1&&X(0,"textarea",4),t&2){let e=C().$implicit,i=C();M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function T$(t,n){if(t&1&&(m(0,"mat-option",7),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function A$(t,n){if(t&1&&(m(0,"mat-select",5),nt(1,T$,2,2,"mat-option",7,tt),v()),t&2){let e=C().$implicit,i=C();M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function R$(t,n){if(t&1&&X(0,"input",6),t&2){let e=C().$implicit,i=C();M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function k$(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function O$(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function N$(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function P$(t,n){if(t&1&&(m(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),b(3),v(),A(4,M$,1,2,"textarea",4)(5,A$,3,3,"mat-select",5)(6,R$,1,3,"input",6),A(7,k$,2,1,"mat-hint"),A(8,O$,2,1,"mat-error"),A(9,N$,2,1,"mat-error"),v()()),t&2){let e,i,r,o,a=n.$implicit,s=C();g(3),fe(a.label),g(),R((e=a.controlType)==="textarea"?4:e==="select"?5:6),g(3),R((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),g(),R((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),g(),R((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}var Gh=class t{innerWidths="0";document=d(j);dialog=d(pa);usuarioService=d(dn);carritoService=d($n);usuario=I(L.usuarioVacio());pedido=I(L.pedidoVacio());detallePedidoList=I([]);constructor(){}ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=Me.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},this.dialog.open(v0,n).afterClosed().subscribe(i=>{this.cargarListas()})}agregarDetallePedido(){let n=Me.getMatDialogConf();n.data={pedido:this.pedido()},this.dialog.open(_0,n).afterClosed().subscribe(i=>{this.cargarListas()})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-carrito"]],decls:27,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(m(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),b(4," Carrito de compras "),v()(),m(5,"table",2),dt(6,3),ue(7,d$,2,2,"th",4)(8,u$,2,4,"td",5),ut(),dt(9,6),ue(10,f$,2,2,"th",4)(11,p$,2,3,"td",5),ut(),dt(12,7),ue(13,h$,2,2,"th",4)(14,m$,3,5,"td",5),ut(),dt(15,8),ue(16,g$,2,0,"th",9)(17,v$,7,0,"td",10),ut(),ue(18,_$,1,0,"tr",11)(19,y$,1,0,"tr",12),v()(),m(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),G("click",function(){return i.pagoCarrito()}),b(24,"Pagar Carrito"),v(),m(25,"button",13),G("click",function(){return i.agregarDetallePedido()}),b(26,"Agregar Producto"),v()()()()()),e&2&&(g(),we("overflow","auto"),g(4),M("dataSource",i.detallePedidoList()),g(13),M("matHeaderRowDef",Fi(5,cR)),g(),M("matRowDefColumns",Fi(6,cR)))},dependencies:[vo,Ys,Dr,wr,ho,qs,tn,ma,Wi,Js,tl,ol,nl,el,al,il,rl,sl,ll,Jr],encapsulation:2})},v0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(j);_snackBar=d(En);dialogRef=d(pi);formConfigs=I([]);matcher=new Wn;formService=d(Gn);dataService=d(Vt);carritoService=d($n);usuarioService=d(dn);pagoCarritoForm;detallePedidoList=I([]);pasarelaList=I([]);totalPedido=I(0);comisionPasarela=I(0);totalPago=I(0);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLineaFormControls(this.pasarelaList());this.pagoCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.cargarDatosForm()}getFormControl(n){return this.pagoCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionChange(n){let e=n.value;this.comisionPasarela.update(i=>e.comision),this.totalPago.update(i=>this.totalPedido()+e.comision)}pagarPedido(){if(this.validarDatos())if(this.totalPago()>0&&this.totalPedido()>0){var n=this.detallePedidoList()[0].pedido;this.carritoService.pedidoPagado(n),Me.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else Me.openSnackBar("Datos Incorrectos","aceptar",this._snackBar);else Me.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.pagoCarritoForm);return n}static \u0275fac=function(e){return new(e||t)(ee(gr))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:32,vars:10,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(m(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),b(5,"Pagar Pedido"),v()(),m(6,"mat-card-content")(7,"form",1),G("ngSubmit",function(){return i.pagarPedido()}),m(8,"mat-grid-list",2)(9,"div")(10,"mat-label"),b(11,"Total de pedido: "),v(),m(12,"mat-label"),b(13),Li(14,"currency"),v()(),nt(15,I$,10,5,"div",null,tt),m(17,"div")(18,"mat-label"),b(19,"comision pasarela: "),v(),m(20,"mat-label"),b(21),Li(22,"currency"),v()(),m(23,"div")(24,"mat-label"),b(25,"Total a pagar: "),v(),m(26,"mat-label"),b(27),Li(28,"currency"),v()(),m(29,"div")(30,"button",3),b(31,"Pagar"),v()()()()()()()()),e&2&&(g(7),M("formGroup",i.pagoCarritoForm),g(6),fe(Vi(14,4,i.totalPedido())),g(2),it(i.formConfigs()),g(6),fe(Vi(22,6,i.comisionPasarela())),g(6),fe(Vi(28,8,i.totalPago())))},dependencies:[Yi,tn,so,yr,Cr,br,Sn,ln,Un,zn,Er,go,xn,qi,Gi,Bn,Hn,$i,vi,nn,gi,ha,Jr],encapsulation:2})},_0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px"}innerWidths="0";document=d(j);_snackBar=d(En);dialogRef=d(pi);formConfigs=I([]);matcher=new Wn;formService=d(Gn);carritoService=d($n);dataService=d(Vt);usuarioService=d(dn);agregarCarritoForm;productoList=I([]);pedido=L.pedidoVacio();ngOnInit(){this.cargarDatos(),this.cargarListas();var n=this.formService.newAgregarDetallePedidoControls(this.productoList());this.agregarCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}getFormControl(n){return this.agregarCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(){var n=this.usuarioService.getUsuarioLoggeado();this.pedido=this.carritoService.getUltimoPedidoUsuario(n)}cargarListas(){this.productoList.update(n=>[...this.carritoService.getProductosStocWeb()])}agregarDetallePedido(){if(this.validarDatos()){let n=this.agregarCarritoForm.value,e=L.detallePedidoVacio();e.pedido=this.pedido,e.cantidad=n.cantidad,e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.dataService.pushDetallePedido(e),Me.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else Me.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCarritoForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ee(gr))};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(b(0,"3"),m(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),b(6,"Agregar Producto"),v()(),m(7,"mat-card-content")(8,"form",1),G("ngSubmit",function(){return i.agregarDetallePedido()}),m(9,"mat-grid-list",2),nt(10,P$,10,5,"mat-card-title",null,tt),m(12,"mat-card-title")(13,"button",3),b(14,"Agregar"),v()()()()()()()()),e&2&&(g(8),M("formGroup",i.agregarCarritoForm),g(2),it(i.formConfigs()))},dependencies:[Yi,tn,so,yr,Cr,br,Sn,ln,Un,zn,Er,go,xn,qi,Gi,Bn,Hn,$i,vi,nn,gi,ha],encapsulation:2})};var cl=class t{documento=d(j);localStorageService=d(cn);theme=I("light");constructor(){ti(()=>{let n=this.theme;this.localStorageService.setItem("app-theme",n()?n():"light");let e=this.documento.documentElement;n()==="dark"?(e.classList.add("dark"),e.classList.remove("light")):(e.classList.add("light"),e.classList.remove("dark"))})}toggleTheme(){this.theme.update(n=>n==="light"?"dark":"light")}setTheme(n){this.theme.set(n)}getTheme(){return this.localStorageService.getItem("app-theme")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac,providedIn:"root"})};function F$(t,n){t&1&&(m(0,"mat-icon"),b(1,"account_circle"),v())}function L$(t,n){t&1&&(m(0,"mat-icon"),b(1,"menu"),v())}function V$(t,n){if(t&1&&(m(0,"button",3),b(1),v()),t&2){let e=n.$implicit;M("routerLink",e.ruta),g(),fe(e.nombre)}}function j$(t,n){if(t&1){let e=rt();m(0,"button",8),G("click",function(){je(e);let r=C();return Be(r.cerrarSesion())}),m(1,"mat-icon"),b(2,"account_circle_off"),v(),m(3,"span"),b(4,"Cerrar Sesion"),v()(),X(5,"mat-divider")}if(t&2){let e=C();M("hidden",e.logged()==!1)}}var Wh=class t{logged=I(!1);isChecked=I(!1);isLightTheme=I(!0);localStorage=d(cn);themeService=d(cl);_router=d(Dn);menusList=I([]);constructor(){}ngOnInit(){this.cargarMenusDeMantenimientos(),this.validarUsuarioLogeado();var n=this.themeService.getTheme();this.isLightTheme.update(e=>n=="light")}onThemeSwitchChange(){this.themeService.toggleTheme()}routerActivated(n){this.validarUsuarioLogeado()}cargarMenusDeMantenimientos(){var n=[];n.push({id:1,ruta:"/menu/login",nombre:"Iniciar Sesion"}),n.push({id:2,ruta:"/menu/productos",nombre:"Productos"}),n.push({id:3,ruta:"/menu/carrito",nombre:"Carrito"}),n.push({id:3,ruta:"/menu/ventas",nombre:"Ventas"}),this.menusList.update(e=>[...n])}homeClick(){let n={queryParams:{nada:"xd"}};this._router.navigate(["/menu/productos"],n)}cerrarSesion(){let n={queryParams:{logged:"false"}};this.localStorage.setItem(L.LOGGED,"false"),this._router.navigate(["/menu/login"],n)}validarUsuarioLogeado(){let n=this.localStorage.getItem(L.LOGGED);this.logged.update(e=>n==!0)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-inicio"]],decls:19,vars:14,consts:[["menu","matMenu"],["matIconButton","",3,"matMenuTriggerFor"],["src","logo4.jpg",3,"click"],["mat-menu-item","","routerLinkActive","active-menu-item",3,"routerLink"],[3,"ngModelChange","change","ngModel"],[1,"container-fluid"],[1,"row"],[3,"activate"],["mat-menu-item","",3,"click","hidden"]],template:function(e,i){if(e&1){let r=rt();m(0,"mat-toolbar")(1,"button",1),A(2,F$,2,0,"mat-icon"),A(3,L$,2,0,"mat-icon"),v(),X(4,"span"),m(5,"span")(6,"img",2),G("click",function(){return i.homeClick()}),v()(),X(7,"span"),v(),m(8,"mat-menu",null,0),nt(10,V$,2,2,"button",3,tt),X(12,"mat-divider"),A(13,j$,6,1),m(14,"mat-slide-toggle",4),Wf("ngModelChange",function(a){return je(r),S_(i.isChecked,a)||(i.isChecked=a),Be(a)}),G("change",function(){return i.onThemeSwitchChange()}),b(15),v()(),m(16,"div",5)(17,"div",6)(18,"router-outlet",7),G("activate",function(a){return i.routerActivated(a)}),v()()()}if(e&2){let r=Pt(9);g(),M("matMenuTriggerFor",r),g(),R(i.logged()?2:-1),g(),R(i.logged()?-1:3),g(),we("margin-left","1em"),g(2),we("height","100%")("max-height","2em"),g(),we("margin-left","1em"),g(3),it(i.menusList()),g(3),R(i.logged()?13:-1),g(),Gf("ngModel",i.isChecked),g(),Se("Colores: ",i.isChecked()?"Oscuro":"Claro")}},dependencies:[$h,sR,Wi,Zs,xd,HA,e0,Hn,Rb,ia,ha,vT,As,Hy,ga,ma],encapsulation:2})};function B$(t,n){if(t&1&&(m(0,"label",1),b(1),v()),t&2){let e=C();g(),fe(e.mensajeLogin)}}function H$(t,n){t&1&&(m(0,"mat-error"),b(1,"Ingrese Usuario"),v())}function U$(t,n){t&1&&(m(0,"mat-error"),b(1,"Ingrese Contrase\xF1a"),v())}var qh=class t{constructor(n,e){this._router=n;this.formBuilder=e;this.loginForm=this.formBuilder.group({username:["",[q.required,q.minLength(3)]],password:["",[q.required,q.minLength(3)]]})}loginForm;mensajeLogin="";_snackBar=d(En);usuarioService=d(dn);localStorage=d(cn);matcher=new Wn;login(){if(this.validarDatos()){var n=this.usuarioService.validarUsuario(this.loginF("username")?.value,this.loginF("password")?.value);if(n&&n.id>1){Me.openSnackBar("Login exitoso","ok",this._snackBar),this.localStorage.setItem(L.LOGGED_USUARIO,n),this.localStorage.setItem(L.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}else Me.openSnackBar("Credenciales incorrectas","ok",this._snackBar)}else Me.openSnackBar("Completar credenciales","ok",this._snackBar)}iniciarInvitado(){var n=this.usuarioService.getUsuarioAnonimo();this.localStorage.setItem(L.LOGGED_USUARIO,n),this.localStorage.setItem(L.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}validarDatos(){var n=this.loginForm.valid;return this.loginF("username")?.valid||(n=!1),this.loginF("password")?.valid||(n=!1),n}loginF(n){return this.loginForm.get(n)}static \u0275fac=function(e){return new(e||t)(ee(Dn),ee(Dh))};static \u0275cmp=O({type:t,selectors:[["app-login"]],decls:28,vars:6,consts:[["hideToggle","","expanded","true",1,"login-container"],[1,"h4"],["id","loginForm",3,"ngSubmit","formGroup"],["cols","1"],["matInput","","required","","formControlName","username",3,"errorStateMatcher"],["matInput","","required","","formControlName","password","type","password",3,"errorStateMatcher"],["mat-stroked-button","","color","primary","type","submit"],["matButton","",3,"click"]],template:function(e,i){if(e&1&&(m(0,"mat-expansion-panel",0)(1,"mat-expansion-panel-header")(2,"mat-panel-title"),b(3," Iniciar Sesi\xF3n "),v()(),m(4,"mat-card")(5,"mat-card-title"),A(6,B$,2,1,"label",1),v(),m(7,"mat-card-content")(8,"form",2),G("ngSubmit",function(){return i.login()}),m(9,"mat-grid-list",3)(10,"mat-card-title")(11,"mat-form-field")(12,"mat-label"),b(13,"Usuario"),v(),X(14,"input",4),A(15,H$,2,0,"mat-error"),v()(),m(16,"mat-card-title")(17,"mat-form-field")(18,"mat-label"),b(19,"Contrase\xF1a"),v(),X(20,"input",5),A(21,U$,2,0,"mat-error"),v()(),m(22,"mat-card-title")(23,"button",6),b(24,"Ingresar"),v()()()()()(),m(25,"mat-action-row")(26,"button",7),G("click",function(){return i.iniciarInvitado()}),b(27,"Continuar como invitado"),v()()()),e&2){let r,o;g(6),R(i.mensajeLogin&&i.mensajeLogin!=""?6:-1),g(2),M("formGroup",i.loginForm),g(6),M("errorStateMatcher",i.matcher),g(),R((r=i.loginF("username"))!=null&&r.errors?15:-1),g(5),M("errorStateMatcher",i.matcher),g(),R((o=i.loginF("password"))!=null&&o.errors?21:-1)}},dependencies:[zh,Dr,TA,wr,ho,tn,yr,Cr,br,Sn,ln,zn,Er,qi,Gi,Bn,Hn,$i,vi,nn,gi],encapsulation:2})};var dR=()=>["sku","nombre","descripcion","precio_venta","estado","opciones"];function z$(t,n){if(t&1){let e=rt();m(0,"button",16),G("click",function(){je(e);let r=C();return Be(r.crearProducto())}),b(1,"Crear Producto"),v()}}function $$(t,n){if(t&1){let e=rt();m(0,"button",16),G("click",function(){je(e);let r=C();return Be(r.crearProducto())}),b(1,"Editar Producto"),v()}}function G$(t,n){t&1&&(m(0,"th",17),b(1,"Codigo"),v()),t&2&&we("text-align","center")}function W$(t,n){if(t&1&&(m(0,"td",18),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",e.sku," ")}}function q$(t,n){t&1&&(m(0,"th",17),b(1,"Nombre"),v()),t&2&&we("text-align","center")}function Y$(t,n){if(t&1&&(m(0,"td",18),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",e.nombre)}}function Q$(t,n){t&1&&(m(0,"th",17),b(1,"Descripcion"),v()),t&2&&we("text-align","center")}function Z$(t,n){if(t&1&&(m(0,"td",18),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",e.descripcion)}}function K$(t,n){t&1&&(m(0,"th",17),b(1,"Precio de Venta"),v()),t&2&&we("text-align","center")}function X$(t,n){if(t&1&&(m(0,"td",18),b(1),Li(2,"currency"),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",Vi(2,3,e.precio_venta)," ")}}function J$(t,n){t&1&&(m(0,"th",17),b(1,"Periodo"),v()),t&2&&we("text-align","center")}function e3(t,n){if(t&1&&(m(0,"td",18),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",e.estado.descripcion," ")}}function t3(t,n){t&1&&(m(0,"th",19),b(1,"OPCIONES"),v())}function n3(t,n){if(t&1){let e=rt();m(0,"td",18)(1,"button",20),G("click",function(){let r=je(e).$implicit,o=C();return Be(o.editarProducto(r))}),m(2,"mat-icon"),b(3,"edit"),v()()()}}function i3(t,n){t&1&&X(0,"tr",21)}function r3(t,n){if(t&1){let e=rt();m(0,"tr",22),G("click",function(){let r=je(e).$implicit,o=C();return Be(o.seleccionarProducto(r))}),v()}}function o3(t,n){t&1&&(m(0,"label",0),b(1,"Agregar Producto"),v())}function a3(t,n){t&1&&(m(0,"label",0),b(1,"Editar Producto"),v())}function s3(t,n){if(t&1&&X(0,"textarea",4),t&2){let e=C().$implicit,i=C();M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function l3(t,n){if(t&1&&(m(0,"mat-option",7),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function c3(t,n){if(t&1&&(m(0,"mat-select",5),nt(1,l3,2,2,"mat-option",7,tt),v()),t&2){let e=C().$implicit,i=C();M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function d3(t,n){if(t&1&&X(0,"input",6),t&2){let e=C().$implicit,i=C();M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function u3(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function f3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function p3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function h3(t,n){if(t&1&&(m(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),b(3),v(),A(4,s3,1,2,"textarea",4)(5,c3,3,3,"mat-select",5)(6,d3,1,3,"input",6),A(7,u3,2,1,"mat-hint"),A(8,f3,2,1,"mat-error"),A(9,p3,2,1,"mat-error"),v()()),t&2){let e,i,r,o,a=n.$implicit,s=C();g(3),fe(a.label),g(),R((e=a.controlType)==="textarea"?4:e==="select"?5:6),g(3),R((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),g(),R((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),g(),R((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}function m3(t,n){t&1&&(m(0,"button",3),b(1,"Agregar"),v())}function g3(t,n){t&1&&(m(0,"button",3),b(1,"Editar"),v())}var Yh=class t{innerWidths="0";document=d(j);dialog=d(pa);dataService=d(Vt);productoSeleccionado=L.productoVacio();productosList=I([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.productosList.update(()=>[...this.dataService.getProductos()])}seleccionarProducto(n){this.productoSeleccionado=n}crearProducto(){let n=Me.getMatDialogConf();n.data={productoSeleccionado:this.productoSeleccionado},this.dialog.open(y0,n).afterClosed().subscribe(i=>{this.cargarListas(),this.productoSeleccionado=L.productoVacio()})}editarProducto(n){this.productoSeleccionado=n,this.crearProducto()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-productos"]],decls:31,vars:9,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-stroked-button","","color","primary"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","nombre"],["matColumnDef","descripcion"],["matColumnDef","precio_venta"],["matColumnDef","estado"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",3,"click",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matMiniFab","",3,"click"],["mat-header-row",""],["mat-row","",3,"click"]],template:function(e,i){e&1&&(m(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-description"),A(4,z$,2,0,"button",2),A(5,$$,2,0,"button",2),v()()(),m(6,"mat-expansion-panel",1)(7,"mat-expansion-panel-header")(8,"mat-panel-title"),b(9," Tabla Productos "),v()(),m(10,"table",3),dt(11,4),ue(12,G$,2,2,"th",5)(13,W$,2,3,"td",6),ut(),dt(14,7),ue(15,q$,2,2,"th",5)(16,Y$,2,3,"td",6),ut(),dt(17,8),ue(18,Q$,2,2,"th",5)(19,Z$,2,3,"td",6),ut(),dt(20,9),ue(21,K$,2,2,"th",5)(22,X$,3,5,"td",6),ut(),dt(23,10),ue(24,J$,2,2,"th",5)(25,e3,2,3,"td",6),ut(),dt(26,11),ue(27,t3,2,0,"th",12)(28,n3,4,0,"td",13),ut(),ue(29,i3,1,0,"tr",14)(30,r3,1,0,"tr",15),v()()()),e&2&&(g(4),R(!i.productoSeleccionado||i.productoSeleccionado.id==0?4:-1),g(),R(!i.productoSeleccionado||i.productoSeleccionado.id>0?5:-1),g(),we("overflow","auto"),g(4),M("dataSource",i.productosList()),g(19),M("matHeaderRowDef",Fi(7,dR)),g(),M("matRowDefColumns",Fi(8,dR)))},dependencies:[vo,Ys,Dr,wr,ho,qs,tn,TT,Wi,Js,tl,ol,nl,el,al,il,rl,sl,ll,Jr],encapsulation:2})},y0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.seleccionarProducto(n.productoSeleccionado)}innerWidths="0";document=d(j);_snackBar=d(En);dataService=d(Vt);dialogRef=d(pi);formConfigs=I([]);matcher=new Wn;formService=d(Gn);productoForm;productoSeleccionado=L.productoVacio();estadoProductoList=I([]);ngOnInit(){this.cargarListas();var n=this.formService.newProductoControls(this.estadoProductoList());this.productoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.seleccionarProductoForm()}getFormControl(n){return this.productoForm.get(n)}compareIds(n,e){return n.id==e.id}seleccionarProducto(n){this.productoSeleccionado=n}seleccionarProductoForm(){this.getFormControl("id")?.setValue(this.productoSeleccionado.id),this.getFormControl("sku")?.setValue(this.productoSeleccionado.sku),this.getFormControl("nombre")?.setValue(this.productoSeleccionado.nombre),this.getFormControl("descripcion")?.setValue(this.productoSeleccionado.descripcion),this.getFormControl("precio_venta")?.setValue(this.productoSeleccionado.precio_venta),this.getFormControl("estado")?.setValue(this.productoSeleccionado.estado)}cargarListas(){this.estadoProductoList.update(n=>[...this.dataService.getEstadosProducto()])}guardarProducto(){if(this.validarDatos()){if(this.productoSeleccionado.id>0){var n=this.productoSeleccionado.id;this.productoSeleccionado=this.productoForm.value,this.productoSeleccionado.id=n,this.dataService.editarProducto(this.productoSeleccionado),Me.openSnackBar("CAMBIOS GUARDADOS EXITOSAMENTE","aceptar",this._snackBar)}else this.productoSeleccionado=this.productoForm.value,this.dataService.pushProducto(this.productoSeleccionado),Me.openSnackBar("NUEVO PRODUCTO CREADO EXITOSAMENTE","aceptar",this._snackBar);this.dialogRef.close()}else Me.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.productoForm),e=parseFloat(this.getFormControl("precio_venta")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("precio_venta")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ee(gr))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:16,vars:5,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(m(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),b(5,"Formulario"),v(),A(6,o3,2,0,"label",0),A(7,a3,2,0,"label",0),v(),m(8,"mat-card-content")(9,"form",1),G("ngSubmit",function(){return i.guardarProducto()}),m(10,"mat-grid-list",2),nt(11,h3,10,5,"mat-card-title",null,tt),m(13,"mat-card-title"),A(14,m3,2,0,"button",3),A(15,g3,2,0,"button",3),v()()()()()()()),e&2&&(g(6),R(!i.productoSeleccionado||i.productoSeleccionado.id==0?6:-1),g(),R(i.productoSeleccionado&&i.productoSeleccionado.id>0?7:-1),g(2),M("formGroup",i.productoForm),g(2),it(i.formConfigs()),g(3),R(!i.productoSeleccionado||i.productoSeleccionado.id==0?14:-1),g(),R(i.productoSeleccionado&&i.productoSeleccionado.id>0?15:-1))},dependencies:[Yi,tn,so,yr,Cr,br,Sn,ln,Un,zn,Er,go,xn,qi,Gi,Bn,Hn,$i,vi,nn,gi],encapsulation:2})};var uR=()=>["producto","cantidad","precio","opciones"];function v3(t,n){t&1&&(m(0,"th",14),b(1,"Producto"),v()),t&2&&we("text-align","center")}function _3(t,n){if(t&1&&(m(0,"td",15),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),ss(" ",e.producto.sku," ",e.producto.nombre," ")}}function y3(t,n){t&1&&(m(0,"th",14),b(1,"Cantidad"),v()),t&2&&we("text-align","center")}function b3(t,n){if(t&1&&(m(0,"td",15),b(1),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",e.cantidad)}}function C3(t,n){t&1&&(m(0,"th",14),b(1,"Precio"),v()),t&2&&we("text-align","center")}function D3(t,n){if(t&1&&(m(0,"td",15),b(1),Li(2,"currency"),v()),t&2){let e=n.$implicit;we("text-align","center"),g(),Se(" ",Vi(2,3,e.precio_unitario_venta))}}function w3(t,n){t&1&&(m(0,"th",16),b(1,"OPCIONES"),v())}function E3(t,n){if(t&1){let e=rt();m(0,"td",15)(1,"button",17),G("click",function(){let r=je(e).$implicit,o=C();return Be(o.aumentarDetallePedido(r))}),m(2,"mat-icon"),b(3,"arrow_circle_up"),v()(),m(4,"button",17),G("click",function(){let r=je(e).$implicit,o=C();return Be(o.disminuirDetallePedido(r))}),m(5,"mat-icon"),b(6,"arrow_circle_down"),v()()()}}function x3(t,n){t&1&&X(0,"tr",18)}function S3(t,n){t&1&&X(0,"tr",19)}function I3(t,n){t&1&&b(0,"Datos Cliente")}function M3(t,n){if(t&1&&X(0,"textarea",8),t&2){let e=C().$implicit,i=C();M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function T3(t,n){if(t&1&&(m(0,"mat-option",11),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function A3(t,n){if(t&1&&(m(0,"mat-select",9),nt(1,T3,2,2,"mat-option",11,tt),v()),t&2){let e=C().$implicit,i=C();M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function R3(t,n){if(t&1&&X(0,"input",10),t&2){let e=C().$implicit,i=C();M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function k3(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function O3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function N3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function P3(t,n){if(t&1&&(m(0,"mat-form-field")(1,"mat-label"),b(2),v(),A(3,M3,1,2,"textarea",8)(4,A3,3,3,"mat-select",9)(5,R3,1,3,"input",10),A(6,k3,2,1,"mat-hint"),A(7,O3,2,1,"mat-error"),A(8,N3,2,1,"mat-error"),v()),t&2){let e,i,r,o,a=n.$implicit,s=C();g(2),fe(a.label),g(),R((e=a.controlType)==="textarea"?3:e==="select"?4:5),g(3),R((i=s.getFormControlCliente(a.name))!=null&&i.errors?6:-1),g(),R((r=s.getFormControlCliente(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),g(),R((o=s.getFormControlCliente(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function F3(t,n){t&1&&b(0,"Tipo Pago")}function L3(t,n){if(t&1&&X(0,"textarea",8),t&2){let e=C().$implicit,i=C();M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function V3(t,n){if(t&1&&(m(0,"mat-option",11),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function j3(t,n){if(t&1){let e=rt();m(0,"mat-select",12),G("selectionChange",function(r){je(e);let o=C(2);return Be(o.onSelectionTipoPagoChange(r))}),nt(1,V3,2,2,"mat-option",11,tt),v()}if(t&2){let e=C().$implicit,i=C();M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function B3(t,n){if(t&1&&X(0,"input",10),t&2){let e=C().$implicit,i=C();M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function H3(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function U3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function z3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function $3(t,n){if(t&1&&(m(0,"mat-form-field")(1,"mat-label"),b(2),v(),A(3,L3,1,2,"textarea",8)(4,j3,3,3,"mat-select",9)(5,B3,1,3,"input",10),A(6,H3,2,1,"mat-hint"),A(7,U3,2,1,"mat-error"),A(8,z3,2,1,"mat-error"),v()),t&2){let e,i,r,o,a=n.$implicit,s=C();g(2),fe(a.label),g(),R((e=a.controlType)==="textarea"?3:e==="select"?4:5),g(3),R((i=s.getFormControlTipoPago(a.name))!=null&&i.errors?6:-1),g(),R((r=s.getFormControlTipoPago(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),g(),R((o=s.getFormControlTipoPago(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function G3(t,n){t&1&&b(0,"Tipo Pago")}function W3(t,n){if(t&1&&X(0,"textarea",8),t&2){let e=C().$implicit,i=C(2);M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function q3(t,n){if(t&1&&(m(0,"mat-option",11),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function Y3(t,n){if(t&1&&(m(0,"mat-select",9),nt(1,q3,2,2,"mat-option",11,tt),v()),t&2){let e=C().$implicit,i=C(2);M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function Q3(t,n){if(t&1&&X(0,"input",10),t&2){let e=C().$implicit,i=C(2);M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Z3(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function K3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function X3(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function J3(t,n){if(t&1&&(m(0,"mat-form-field")(1,"mat-label"),b(2),v(),A(3,W3,1,2,"textarea",8)(4,Y3,3,3,"mat-select",9)(5,Q3,1,3,"input",10),A(6,Z3,2,1,"mat-hint"),A(7,K3,2,1,"mat-error"),A(8,X3,2,1,"mat-error"),v()),t&2){let e,i,r,o,a=n.$implicit,s=C(2);g(2),fe(a.label),g(),R((e=a.controlType)==="textarea"?3:e==="select"?4:5),g(3),R((i=s.getFormControlTipoPago(a.name))!=null&&i.errors?6:-1),g(),R((r=s.getFormControlTipoPago(a.name))!=null&&r.errors&&a.controlType==="select"?7:-1),g(),R((o=s.getFormControlTipoPago(a.name))!=null&&o.errors&&a.controlType!="select"?8:-1)}}function e4(t,n){if(t&1&&(m(0,"mat-step",2)(1,"form",3),ue(2,G3,1,0,"ng-template",4),nt(3,J3,9,5,"mat-form-field",null,tt),m(5,"div")(6,"button",6),b(7,"Back"),v(),m(8,"button",5),b(9,"Next"),v()()()()),t&2){let e=C();M("stepControl",e.datosTarjetaForm),g(),M("formGroup",e.datosTarjetaForm),g(2),it(e.formConfigsTarjeta())}}function t4(t,n){t&1&&b(0,"Done")}var D0=".fade-in[_ngcontent-%COMP%]{opacity:0;transform:translateY(10px);transition:opacity .4s ease,transform .4s ease}div[_ngcontent-%COMP%]{opacity:1;transform:translateY(0)}.fade-out[_ngcontent-%COMP%]{opacity:1;transform:translateY(0);transition:opacity .4s ease,transform .4s ease}.fade-out.leave-active[_ngcontent-%COMP%]{opacity:0;transform:translateY(-10px)}";function n4(t,n){if(t&1&&X(0,"textarea",4),t&2){let e=C().$implicit,i=C();M("formControlName",e.name)("errorStateMatcher",i.matcher)}}function i4(t,n){if(t&1&&(m(0,"mat-option",7),b(1),v()),t&2){let e=n.$implicit;M("value",e.value),g(),fe(e.name)}}function r4(t,n){if(t&1&&(m(0,"mat-select",5),nt(1,i4,2,2,"mat-option",7,tt),v()),t&2){let e=C().$implicit,i=C();M("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),g(),it(e.options)}}function o4(t,n){if(t&1&&X(0,"input",6),t&2){let e=C().$implicit,i=C();M("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function a4(t,n){if(t&1&&(m(0,"mat-hint"),b(1),v()),t&2){let e=C().$implicit;g(),fe(e.label)}}function s4(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Seleccione ",e.label)}}function l4(t,n){if(t&1&&(m(0,"mat-error"),b(1),v()),t&2){let e=C().$implicit;g(),Se("Ingrese ",e.label)}}function c4(t,n){if(t&1&&(m(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),b(3),v(),A(4,n4,1,2,"textarea",4)(5,r4,3,3,"mat-select",5)(6,o4,1,3,"input",6),A(7,a4,2,1,"mat-hint"),A(8,s4,2,1,"mat-error"),A(9,l4,2,1,"mat-error"),v()()),t&2){let e,i,r,o,a=n.$implicit,s=C();g(3),fe(a.label),g(),R((e=a.controlType)==="textarea"?4:e==="select"?5:6),g(3),R((i=s.getFormControl(a.name))!=null&&i.errors?7:-1),g(),R((r=s.getFormControl(a.name))!=null&&r.errors&&a.controlType==="select"?8:-1),g(),R((o=s.getFormControl(a.name))!=null&&o.errors&&a.controlType!="select"?9:-1)}}var Qh=class t{constructor(n){this._router=n}_snackBar=d(En);innerWidths="0";document=d(j);dialog=d(pa);usuarioService=d(dn);carritoService=d($n);usuario=I(L.usuarioVacio());pedido=I(L.pedidoVacio());detallePedidoList=I([]);ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=Me.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},n.width="90vh",n.height="90vh",n.maxWidth="100vh",this.dialog.open(b0,n).afterClosed().subscribe(i=>{this.cargarListas()})}agregarDetallePedido(){if(this.usuario()&&this.usuario().id>0){let n=Me.getMatDialogConf();n.data={pedido:this.pedido()},this.dialog.open(C0,n).afterClosed().subscribe(i=>{this.cargarListas()})}else{Me.openSnackBar("Usuario no registrado","aceptar",this._snackBar);let n={queryParams:{logged:"true"}};this._router.navigate(["/menu/login"],n)}}static \u0275fac=function(e){return new(e||t)(ee(Dn))};static \u0275cmp=O({type:t,selectors:[["app-ventas"]],decls:27,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(m(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),b(4," Ventas "),v()(),m(5,"table",2),dt(6,3),ue(7,v3,2,2,"th",4)(8,_3,2,4,"td",5),ut(),dt(9,6),ue(10,y3,2,2,"th",4)(11,b3,2,3,"td",5),ut(),dt(12,7),ue(13,C3,2,2,"th",4)(14,D3,3,5,"td",5),ut(),dt(15,8),ue(16,w3,2,0,"th",9)(17,E3,7,0,"td",10),ut(),ue(18,x3,1,0,"tr",11)(19,S3,1,0,"tr",12),v()(),m(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),G("click",function(){return i.pagoCarrito()}),b(24,"Pagar Carrito"),v(),m(25,"button",13),G("click",function(){return i.agregarDetallePedido()}),b(26,"Agregar Producto"),v()()()()()),e&2&&(g(),we("overflow","auto"),g(4),M("dataSource",i.detallePedidoList()),g(13),M("matHeaderRowDef",Fi(5,uR)),g(),M("matRowDefColumns",Fi(6,uR)))},dependencies:[vo,Ys,Dr,wr,ho,qs,tn,ma,Wi,Js,tl,ol,nl,el,al,il,rl,sl,ll,Jr],styles:[D0]})},b0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(j);_snackBar=d(En);dialogRef=d(pi);formConfigsCliente=I([]);formConfigsTipoPago=I([]);formConfigsTarjeta=I([]);matcher=new Wn;formService=d(Gn);dataService=d(Vt);carritoService=d($n);usuarioService=d(dn);datosClienteForm;datosTarjetaForm;tipoPagoForm;detallePedidoList=I([]);pasarelaList=I([]);totalPedido=I(0);comisionPasarela=I(0);totalPago=I(0);esPagoTarjeta=I(!1);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLocalClienteFormControls(),e=this.formService.newVentaLocalTipoPagoFormControls(),i=this.formService.newVentaLocalFormControls(this.pasarelaList());this.datosClienteForm=this.formService.getFormGroup(n),this.tipoPagoForm=this.formService.getFormGroup(e),this.datosTarjetaForm=this.formService.getFormGroup(i),this.formConfigsCliente.update(r=>[...n]),this.formConfigsTipoPago.update(r=>[...e]),this.formConfigsTarjeta.update(r=>[...i]),this.cargarDatosForm()}getFormControlCliente(n){return this.datosClienteForm.get(n)}getFormControlTipoPago(n){return this.tipoPagoForm.get(n)}getFormControlTarjeta(n){return this.datosTarjetaForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionTipoPagoChange(n){let e=n.value;console.log("selected val"),console.log(e),e==2&&this.esPagoTarjeta.update(i=>!0),e==1&&this.esPagoTarjeta.update(i=>!1)}pagarPedido(){let n=!1;if(this.validarDatosCliente())if(this.validarDatosFormaPago())if(this.esPagoTarjeta()?this.validarDatosTarjeta()?n=!0:Me.openSnackBar("Datos de tarjeta incorrectos","aceptar",this._snackBar):n=!0,n&&this.totalPago()>0&&this.totalPedido()>0){var e=this.detallePedidoList()[0].pedido;this.carritoService.pedidoPagado(e),Me.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else Me.openSnackBar("Datos de cliente incorrectos","aceptar",this._snackBar);else Me.openSnackBar("Datos de forma de pago incorrectos","aceptar",this._snackBar);else Me.openSnackBar("Datos de cliente incorrectos","ok",this._snackBar)}validarDatosCliente(){var n=this.formService.validateFormControls(this.datosClienteForm);return n}validarDatosFormaPago(){var n=this.formService.validateFormControls(this.tipoPagoForm);return n}validarDatosTarjeta(){var n=this.formService.validateFormControls(this.datosTarjetaForm);return n}static \u0275fac=function(e){return new(e||t)(ee(gr))};static \u0275cmp=O({type:t,selectors:[["dialog-crear"]],decls:34,vars:5,consts:[["stepper",""],["orientation","vertical","linear","true"],[3,"stepControl"],[3,"formGroup"],["matStepLabel",""],["matButton","","matStepperNext",""],["matButton","","matStepperPrevious",""],["matButton","",3,"click"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"]],template:function(e,i){if(e&1){let r=rt();m(0,"mat-dialog-content")(1,"mat-stepper",1,0)(3,"mat-step",2)(4,"form",3),ue(5,I3,1,0,"ng-template",4),nt(6,P3,9,5,"mat-form-field",null,tt),m(8,"div")(9,"button",5),b(10,"Next"),v()()()(),m(11,"mat-step",2)(12,"form",3),ue(13,F3,1,0,"ng-template",4),nt(14,$3,9,5,"mat-form-field",null,tt),m(16,"div")(17,"button",6),b(18,"Back"),v(),m(19,"button",5),b(20,"Next"),v()()()(),A(21,e4,10,2,"mat-step",2),m(22,"mat-step"),ue(23,t4,1,0,"ng-template",4),m(24,"p"),b(25,"Pagar Pedido"),v(),m(26,"div")(27,"button",7),G("click",function(){return i.pagarPedido()}),b(28,"Pagar"),v()(),m(29,"div")(30,"button",6),b(31,"Back"),v(),m(32,"button",7),G("click",function(){je(r);let a=Pt(2);return Be(a.reset())}),b(33,"Reset"),v()()()()()}e&2&&(g(3),M("stepControl",i.datosClienteForm),g(),M("formGroup",i.datosClienteForm),g(2),it(i.formConfigsCliente()),g(5),M("stepControl",i.tipoPagoForm),g(),M("formGroup",i.tipoPagoForm),g(2),it(i.formConfigsTipoPago()),g(7),R(i.esPagoTarjeta()?21:-1))},dependencies:[Yi,tn,so,Sn,ln,Un,zn,go,xn,qi,Gi,Bn,Hn,$i,vi,nn,gi,oR,h0,Md,m0,iR,rR],styles:[D0]})},C0=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px"}innerWidths="0";document=d(j);_snackBar=d(En);dialogRef=d(pi);formConfigs=I([]);matcher=new Wn;formService=d(Gn);carritoService=d($n);dataService=d(Vt);usuarioService=d(dn);agregarCarritoForm;productoList=I([]);pedido=L.pedidoVacio();ngOnInit(){this.cargarDatos(),this.cargarListas();var n=this.formService.newAgregarDetallePedidoControls(this.productoList());this.agregarCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n])}getFormControl(n){return this.agregarCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(){var n=this.usuarioService.getUsuarioLoggeado();this.pedido=this.carritoService.getUltimoPedidoUsuario(n)}cargarListas(){this.productoList.update(n=>[...this.carritoService.getProductosStocLocal()])}agregarDetallePedido(){if(this.validarDatos()){let n=this.agregarCarritoForm.value,e=L.detallePedidoVacio();e.pedido=this.pedido,e.cantidad=n.cantidad,e.producto=n.producto,e.precio_unitario_venta=e.producto.precio_venta,this.dataService.pushDetallePedido(e),Me.openSnackBar("Producto Agregado","aceptar",this._snackBar),this.dialogRef.close()}else Me.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.agregarCarritoForm),e=parseFloat(this.getFormControl("cantidad")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("cantidad")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ee(gr))};static \u0275cmp=O({type:t,selectors:[["dialog-agregar"]],decls:15,vars:1,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(b(0,"3"),m(1,"mat-dialog-content")(2,"div")(3,"mat-card")(4,"mat-card-title")(5,"label",0),b(6,"Agregar Producto"),v()(),m(7,"mat-card-content")(8,"form",1),G("ngSubmit",function(){return i.agregarDetallePedido()}),m(9,"mat-grid-list",2),nt(10,c4,10,5,"mat-card-title",null,tt),m(12,"mat-card-title")(13,"button",3),b(14,"Agregar"),v()()()()()()()()),e&2&&(g(8),M("formGroup",i.agregarCarritoForm),g(2),it(i.formConfigs()))},dependencies:[Yi,tn,so,yr,Cr,br,Sn,ln,Un,zn,Er,go,xn,qi,Gi,Bn,Hn,$i,vi,nn,gi,ha],styles:[D0]})};var fR=[{path:"",redirectTo:"/menu",pathMatch:"full"},{path:"menu",component:Wh,children:[{path:"",redirectTo:"/menu/login",pathMatch:"full"},{path:"login",component:qh},{path:"productos",component:Yh},{path:"carrito",component:Gh},{path:"ventas",component:Qh}]}];var d4=[Vt,Gn,cn,dn,cl,$n],pR={providers:[d4,{provide:db,useValue:{hasBackdrop:!1}},bg(),Uy(fR,zy()),xI(EI())]};var Zh=class t{title=I("techstoresv");static \u0275fac=function(e){return new(e||t)};static \u0275cmp=O({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&X(0,"router-outlet")},dependencies:[ia],styles:['[_nghost-%COMP%]{--bright-blue: oklch(51.01% .274 263.83);--electric-violet: oklch(53.18% .28 296.97);--french-violet: oklch(47.66% .246 305.88);--vivid-pink: oklch(69.02% .277 332.77);--hot-red: oklch(61.42% .238 15.34);--orange-red: oklch(63.32% .24 31.68);--gray-900: oklch(19.37% .006 300.98);--gray-700: oklch(36.98% .014 302.71);--gray-400: oklch(70.9% .015 304.04);--red-to-pink-to-purple-vertical-gradient: linear-gradient( 180deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--red-to-pink-to-purple-horizontal-gradient: linear-gradient( 90deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--pill-accent: var(--bright-blue);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol;box-sizing:border-box;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;display:block;height:100dvh}h1[_ngcontent-%COMP%]{font-size:3.125rem;color:var(--gray-900);font-weight:500;line-height:100%;letter-spacing:-.125rem;margin:0;font-family:Inter Tight,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol}p[_ngcontent-%COMP%]{margin:0;color:var(--gray-700)}main[_ngcontent-%COMP%]{width:100%;min-height:100%;display:flex;justify-content:center;align-items:center;padding:1rem;box-sizing:inherit;position:relative}.angular-logo[_ngcontent-%COMP%]{max-width:9.2rem}.content[_ngcontent-%COMP%]{display:flex;justify-content:space-around;width:100%;max-width:700px;margin-bottom:3rem}.content[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin-top:1.75rem}.content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin-top:1.5rem}.divider[_ngcontent-%COMP%]{width:1px;background:var(--red-to-pink-to-purple-vertical-gradient);margin-inline:.5rem}.pill-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:start;flex-wrap:wrap;gap:1.25rem}.pill[_ngcontent-%COMP%]{display:flex;align-items:center;--pill-accent: var(--bright-blue);background:color-mix(in srgb,var(--pill-accent) 5%,transparent);color:var(--pill-accent);padding-inline:.75rem;padding-block:.375rem;border-radius:2.75rem;border:0;transition:background .3s ease;font-family:var(--inter-font);font-size:.875rem;font-style:normal;font-weight:500;line-height:1.4rem;letter-spacing:-.00875rem;text-decoration:none;white-space:nowrap}.pill[_ngcontent-%COMP%]:hover{background:color-mix(in srgb,var(--pill-accent) 15%,transparent)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+1){--pill-accent: var(--bright-blue)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+2){--pill-accent: var(--electric-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+3){--pill-accent: var(--french-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+4), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+5), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+6){--pill-accent: var(--hot-red)}.pill-group[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{margin-inline-start:.25rem}.social-links[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.73rem;margin-top:1.5rem}.social-links[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{transition:fill .3s ease;fill:var(--gray-400)}.social-links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover   svg[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{fill:var(--gray-900)}@media screen and (max-width:650px){.content[_ngcontent-%COMP%]{flex-direction:column;width:max-content}.divider[_ngcontent-%COMP%]{height:1px;width:100%;background:var(--red-to-pink-to-purple-horizontal-gradient);margin-block:1.5rem}}']})};oy(Zh,pR).catch(t=>console.error(t));
