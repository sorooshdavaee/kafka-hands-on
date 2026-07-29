using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Order.Domain.Orders;

namespace Order.Infrastructure.Persistence.Configurations;

public sealed class OrderConfiguration : IEntityTypeConfiguration<Domain.Orders.Order>
{
    public void Configure(EntityTypeBuilder<Domain.Orders.Order> builder)
    {
        builder.ToTable("orders");
        builder.HasKey(x => x.Id);
        builder.Property(x => x.CustomerId).HasMaxLength(100).IsRequired();
        builder.Property(x => x.CustomerName).HasMaxLength(200).IsRequired();
        builder.Property(x => x.Status).HasConversion<string>().HasMaxLength(32);
        builder.Property(x => x.Discount).HasPrecision(18, 2);
        builder.Property(x => x.CreatedAt).IsRequired();

        builder.OwnsMany(x => x.Lines, line =>
        {
            line.ToTable("order_lines");
            line.WithOwner().HasForeignKey("OrderId");
            line.Property<Guid>("Id");
            line.HasKey("Id");
            line.Property(l => l.ProductId).HasMaxLength(100).IsRequired();
            line.Property(l => l.Quantity).IsRequired();
            line.Property(l => l.UnitPrice).HasPrecision(18, 2);
        });

        builder.Ignore(x => x.DomainEvents);
        builder.Ignore(x => x.TotalAmount);
        builder.HasIndex(x => x.CustomerId);
        builder.Navigation(x => x.Lines).UsePropertyAccessMode(PropertyAccessMode.Field);
    }
}
