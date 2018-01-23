package com.septima.application;

import com.septima.application.endpoint.Answer;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Scope {

    private static volatile Scope instance;
    private final ThreadLocal<Context> context = new ThreadLocal<>();
    private final int maximumLpcQueueSize;
    private final Context globalContext;

    private Scope(Config aConfig) {
        maximumLpcQueueSize = aConfig.getMaximumLpcQueueSize();
        ExecutorService executor = Config.lookupExecutor();
        SubmissionPublisher<Runnable> globalPublisher = new SubmissionPublisher<>(executor, maximumLpcQueueSize);
        globalPublisher.subscribe(new Subscriber("Global"));
        globalContext = new Context(globalPublisher);
    }

    private static void init(Config aConfig) {
        if (instance != null) {
            throw new IllegalStateException("Scope can be initialized only once.");
        }
        instance = new Scope(aConfig);
    }

    private static void done() {
        if (instance == null) {
            throw new IllegalStateException("Extra scope shutdown attempt detected.");
        }
        instance = null;
    }

    private static Scope getInstance() {
        return instance;
    }

    public static <R> CompletableFuture<R> bind(CompletableFuture<R> foreign) {
        Context presentContext = instance.present();
        Objects.requireNonNull(presentContext, "Scope context must present while future's bind");
        return presentContext.bind(foreign);
    }

    public static <A, R> CompletableFuture<R> global(Supplier<A> factory, String key, Function<A, R> action) {
        return Scope.getInstance().globalContext.apply(action, key, factory, false);
    }

    public static <A, R> CompletableFuture<R> session(Supplier<A> factory, String key, Function<A, R> action, Answer answer) {
        return Context.of(answer.getRequest().getSession()).apply(action, key, factory, false);
    }

    private Context present() {
        return context.get();
    }

    private Context createContext() {
        ExecutorService executor = Config.lookupExecutor();
        SubmissionPublisher<Runnable> publisher = new SubmissionPublisher<>(executor, maximumLpcQueueSize);
        publisher.subscribe(new Subscriber("Septima scope"));
        return new Context(publisher);
    }

    public static class Context implements Serializable {

        private static final long serialVersionUID = 1L;

        private static final String ATTRIBUTE = "septima.lpc.context";
        private transient SubmissionPublisher<Runnable> publisher;
        private Map<String, Object> instances = new ConcurrentHashMap<>();

        Context(SubmissionPublisher<Runnable> aPublisher) {
            publisher = aPublisher;
        }

        public static Context of(HttpSession aSession) {
            Context ctx = (Context) aSession.getAttribute(ATTRIBUTE);
            Objects.requireNonNull(ctx, "Scope context sould be associated with http session");
            return ctx;
        }

        private void writeObject(java.io.ObjectOutputStream out)
                throws IOException {
            out.writeObject(instances);
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            instances = (Map<String, Object>) in.readObject();
            ExecutorService executor = Config.lookupExecutor();
            SubmissionPublisher<Runnable> readPublisher = new SubmissionPublisher<>(executor, Scope.getInstance().maximumLpcQueueSize);
            readPublisher.subscribe(new Subscriber("Septima scope"));
            publisher = readPublisher;
        }

        private <A, R> Function<A, R> discover(String aKey) {
            return null;
        }

        private void in(Runnable action) {
            Scope scope = Scope.getInstance();
            publisher.submit(() -> {
                if (scope.context.get() != null) {
                    throw new IllegalStateException("Foreign Scope context detected");
                } else {
                    scope.context.set(this);
                    try {
                        action.run();
                    } finally {
                        scope.context.set(null);
                    }
                }
            });
        }

        private <A, R> CompletableFuture<R> apply(Function<A, R> action, String key, Supplier<A> factory, boolean sameContext) {
            Scope scope = Scope.getInstance();
            Context wasContext = scope.context.get() != null ? scope.context.get() : sameContext ? this : null;
            Objects.requireNonNull(wasContext, "Scope context must present while Scope call");
            CompletableFuture<R> result = new CompletableFuture<>();
            in(() -> {
                try {
                    A a = (A) instances.computeIfAbsent(key, k -> factory.get());
                    R r = action.apply(a);
                    wasContext.in(() -> result.complete(r));
                } catch (Throwable t) {
                    wasContext.in(() -> result.completeExceptionally(t));
                }
            });
            return result;
        }

        private <R> CompletableFuture<R> bind(CompletableFuture<R> foreign) {
            CompletableFuture<R> bound = new CompletableFuture<>();
            foreign
                    .thenAccept(r -> in(() -> bound.complete(r)))
                    .exceptionally(ex -> {
                        in(() -> bound.completeExceptionally(ex));
                        return null;
                    });
            return bound;
        }
    }

    public static class Init implements ServletContextListener {

        @Override
        public void contextInitialized(ServletContextEvent anEvent) {
            init(Config.parse(anEvent.getServletContext()));
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            done();
        }
    }

    public static class SessionInit implements HttpSessionListener {


        @Override
        public void sessionCreated(HttpSessionEvent anEvent) {
            anEvent.getSession().setAttribute(Context.ATTRIBUTE, Scope.getInstance().createContext());
        }

        @Override
        public void sessionDestroyed(HttpSessionEvent anEvent) {
            anEvent.getSession().removeAttribute(Context.ATTRIBUTE);
        }
    }

    private static class Subscriber implements Flow.Subscriber<Runnable> {

        private final String name;
        private volatile Flow.Subscription subscription;

        private Subscriber(String aName) {
            name = aName;
        }

        @Override
        public void onSubscribe(Flow.Subscription aSubscription) {
            subscription = aSubscription;
            subscription.request(1);
        }

        @Override
        public void onNext(Runnable item) {
            subscription.request(1);
            item.run();
        }

        @Override
        public void onComplete() {
            Logger.getLogger(Scope.class.getName()).log(Level.INFO, "'" + name + "' requests processing is terminated.");
        }

        @Override
        public void onError(Throwable throwable) {
            Logger.getLogger(Scope.class.getName()).log(Level.SEVERE, throwable.getMessage(), throwable);
        }
    }
}
