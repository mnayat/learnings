namespace EYMP.Services.Equity.API.Subscribers
{
    using  System.Threading.Tasks;

    public interface ITripLegSubscriber
    {
        Task Register();
    }
}
